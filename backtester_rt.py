import trade_data
import pandas as pd
import numpy as np
import sys
import os
from time import time
from common_functions import round_up, round_dn, format_float, ts_to_day


def find_precision(vals: np.array, md: int = 10):
    cvals = vals[::len(vals)//100]
    return sorted([len(format_float(round(e, md)).split('.')[1])
                   for e in cvals])[int(len(cvals) * 0.95)]


def to_f(str_val):
    try:
        return float(str_val)
    except:
        return str_val[0] == 'T'


def iterate_trades(symbols: [str], n_days: int):
    base_path = 'historical_data/raw_trades/'
    ts = time() - 60 * 60 * 24 * n_days
    day = ts_to_day(ts)
    end_day = ts_to_day(time())
    while day < end_day:
        print(day)
        day_trades = []
        for s in symbols:
            s_ = s.replace('/', '_')
            filepath = f'{base_path}{s_}/{day}.csv'
            with open(filepath) as f:
                lines = f.readlines()
            header = lines[0].strip().split(',')
            lines = [line.split(',') for line in lines[1:]]
            day_trades += [{**{'symbol': s},
                            **{header[i]: to_f(line[i]) for i in range(len(header))}}
                           for line in lines]
        yield sorted(day_trades, key=lambda x: x['timestamp'])
        ts += 60 * 60 * 24
        day = ts_to_day(ts)


def calc_ema_from_raw_trades(rt: pd.DataFrame, spans: [float]) -> pd.DataFrame:
    '''
    ema span in minutes
    rt timestamps in milliseconds
    '''
    try:
        _ = iter(spans)
    except TypeError:
        spans = [spans]
    df = rt[['price', 'timestamp']].join(pd.Series(rt.index, index=rt.index))
    df.loc[:, 'timestamp'] = df['timestamp'] // 1000 * 1000 # seconds
    seconds = df.groupby('timestamp').last().reindex(
        np.arange(df['timestamp'].iloc[0], df['timestamp'].iloc[-1] + 1, 1000)).fillna(method='ffill')
    emas = pd.DataFrame(
        {'ema_{:.0f}m'.format(span): seconds['price'].ewm(span=(span * 60), adjust=False).mean()
         for span in spans})
    reindexed = emas.reindex(df['timestamp'])
    reindexed.index = rt.index
    return reindexed


def create_df(symbols: [str],
              n_days: int,
              settings: dict,
              no_download: bool = False):
    dfs = []
    for s in symbols:
        coin, quot = s.split('/')
        print('preparing', s)
        rt = trade_data.fetch_raw_trades(s, n_days=n_days, no_download=no_download)
        print('precision')
        precision = find_precision(rt.price)
        print('emas')
        start_ts = time()
        rt_emas = calc_ema_from_raw_trades(rt, settings['ema_spans_minutes'])
        print('elapsed seconds calc emas', round(time() - start_ts, 2))
        ema_min = rt_emas.min(axis=1)
        ema_max = rt_emas.max(axis=1)
        long_entry = round_dn(ema_min * (1 - settings['coins'][coin]['entry_spread']), precision)
        shrt_entry = round_up(ema_max * (1 + settings['coins'][coin]['entry_spread']), precision)
        long_exit = round_up(ema_max, precision)
        shrt_exit = round_dn(ema_min, precision)
        rt.loc[:, 'entry_price'] = long_entry.where(rt.is_buyer_maker, shrt_entry)
        rt.loc[:, 'exit_price'] = long_exit.where(~rt.is_buyer_maker, shrt_exit)
        rt.loc[:, 'entry'] = ((rt.is_buyer_maker & (rt.price < long_entry)) |
                              (~rt.is_buyer_maker & (rt.price > shrt_entry)))
        rt = rt[((rt.is_buyer_maker & (rt.price < shrt_exit)) |
                (~rt.is_buyer_maker & (rt.price > long_exit)))]
        rt.loc[:, 'symbol'] = np.repeat(s, len(rt))
        dfs.append(rt.set_index('timestamp'))
    return pd.concat(dfs, axis=0).sort_index()


def backtest(df: pd.DataFrame, settings: dict):
    symbols = list(df.symbol.unique())
    quot = symbols[0].split('/')[1]
    df_mid = df.iloc[int(len(df) * 0.5):int(len(df) * 0.55)]
    precisions = {s: find_precision(df_mid[df_mid.symbol == s].price.values) for s in symbols}

    ppctminus = {f'{c}/{quot}': 1 - settings['coins'][c]['profit_pct'] for c in settings['coins']}
    ppctplus = {f'{c}/{quot}': 1 + settings['coins'][c]['profit_pct'] for c in settings['coins']}

    s2c = {s: s.split('/')[0] for s in symbols}

    balance = {s2c[s]: 0.0 for s in s2c}
    balance[quot] = settings['start_quot']
    balance_ito_quot = {c: balance[c] for c in balance}

    acc_equity_quot = settings['start_quot']
    acc_debt_quot = 0.0

    entries_list = []
    exits_list = []
    exit_prices_list = []

    prev_long_entry_ts = {s: 0 for s in symbols}
    prev_shrt_entry_ts = {s: 0 for s in symbols}

    long_cost = {s: 0.0 for s in symbols}
    long_amount = {s: 0.0 for s in symbols}
    shrt_cost = {s: 0.0 for s in symbols}
    shrt_amount = {s: 0.0 for s in symbols}

    fee = 1 - settings['fee']
    margin = settings['margin'] - 1
    exponent = settings['entry_vol_modifier_exponent']
    max_multiplier = settings['min_exit_cost_multiplier'] / 2
    account_equity_pct_per_symbol_per_hour = {c: settings['coins'][c]['account_equity_pct_per_hour']
                                              for c in settings['coins']}
    millis_wait_until_next_long_entry = {s: 0 for s in symbols}
    millis_wait_until_next_shrt_entry = {s: 0 for s in symbols}

    coins_long = set([c for c in settings['coins'] if settings['coins'][c]['long']])
    coins_shrt = set([c for c in settings['coins'] if settings['coins'][c]['shrt']])

    balance_list = []

    last_price = {s: 0.0 for s in s2c}

    start_ts, end_ts = df.index[0], df.index[-1]
    ts_range = end_ts - start_ts
    k = 0

    hour_to_millis = 60 * 60 * 1000

    for row in df.itertuples():
        s = row.symbol
        last_price[s] = row.price
        coin = s2c[s]
        default_cost = max(
            acc_equity_quot * settings['coins'][coin]['account_equity_pct_per_trade'],
            settings['min_quot_cost']
        )
        credit_avbl_quot = max(0.0, acc_equity_quot * margin - acc_debt_quot)

        if row.entry:
            if row.is_buyer_maker:
                if coin in coins_long and \
                        row.Index - prev_long_entry_ts[s] >= millis_wait_until_next_long_entry[s]:
                    cost = default_cost
                    if long_cost[s] > 0.0:
                        long_exit_price = \
                            long_cost[s] / long_amount[s] if long_amount[s] > 0.0 else row.price
                        cost *= max(
                            1.0,
                            min(max_multiplier, (long_exit_price / row.price) ** exponent)
                        )
                    quot_avbl = max(0.0, balance[quot])
                    borrow_amount = max(0.0, min(cost - quot_avbl, credit_avbl_quot))
                    cost = min(quot_avbl + borrow_amount, cost)
                    if cost >= settings['min_quot_cost']:
                        credit_avbl_quot -= borrow_amount
                        amount = cost / row.entry_price
                        balance[quot] -= cost
                        balance[coin] += amount * fee
                        entries_list.append({'symbol': s, 'timestamp': row.Index, 'side': 'buy',
                                             'amount': amount, 'price': row.entry_price,
                                             'cost': cost})
                        long_cost[s] += cost
                        long_amount[s] += amount
                        prev_long_entry_ts[s] = row.Index
                        millis_wait_until_next_long_entry[s] = (default_cost * hour_to_millis) / \
                            (account_equity_pct_per_symbol_per_hour[coin] * acc_equity_quot)
            else:
                if coin in coins_shrt and \
                        row.Index - prev_shrt_entry_ts[s] >= millis_wait_until_next_shrt_entry[s]:
                    cost = default_cost
                    if shrt_cost[s] > 0.0:
                        shrt_exit_price = \
                            shrt_cost[s] / shrt_amount[s] if shrt_amount[s] > 0.0 else row.price
                        cost *= max(
                            1.0,
                            min(max_multiplier, (row.price / shrt_exit_price) ** exponent)
                        )
                    coin_avbl_quot = max(0.0, balance[coin]) * row.entry_price
                    borrow_amount_quot = max(0.0, min(cost - coin_avbl_quot, credit_avbl_quot))
                    cost = min(coin_avbl_quot + borrow_amount_quot, cost)
                    if cost >= settings['min_quot_cost']:
                        credit_avbl_quot -= borrow_amount_quot
                        amount = cost / row.entry_price
                        balance[coin] -= amount
                        balance[quot] += cost * fee
                        entries_list.append({'symbol': s, 'timestamp': row.Index, 'side': 'sell',
                                             'amount': amount, 'price': row.entry_price,
                                             'cost': cost})
                        shrt_cost[s] += cost
                        shrt_amount[s] += amount
                        prev_shrt_entry_ts[s] = row.Index
                        millis_wait_until_next_shrt_entry[s] = (default_cost * hour_to_millis) / \
                            (account_equity_pct_per_symbol_per_hour[coin] * acc_equity_quot)
        bag_ratio = ((long_amount[s] - shrt_amount[s]) * row.price) / acc_equity_quot
        if row.is_buyer_maker:
            min_exit_cost = default_cost * settings['min_exit_cost_multiplier']
            try:
                shrt_vwap = shrt_cost[s] / shrt_amount[s]
            except ZeroDivisionError:
                shrt_vwap = 10e10
            profit_pct = 1 - min(0.1, max(settings['coins'][coin]['profit_pct'],
                                          bag_ratio * settings['profit_pct_multiplier']))
            exit_price = min(row.exit_price, round_dn(shrt_vwap * profit_pct, precisions[s]))
            exit_cost = shrt_amount[s] * exit_price
            exit_prices_list.append({'timestamp': row.Index, 'symbol': s, 'side': 'buy',
                                     'price': exit_price})
            if coin in coins_shrt and shrt_amount[s] * exit_price >= min_exit_cost:
                if row.price < exit_price and exit_cost >= min_exit_cost:
                    quot_avbl = max(0.0, balance[quot])
                    borrow_amount = max(0.0, min(exit_cost - quot_avbl, credit_avbl_quot))
                    exit_cost = min(quot_avbl + borrow_amount, exit_cost)
                    if exit_cost >= min_exit_cost:
                        balance[quot] -= exit_cost
                        exit_amount = exit_cost / exit_price
                        balance[coin] += exit_amount * fee
                        exits_list.append({'symbol': s, 'timestamp': row.Index, 'side': 'buy',
                                           'amount': exit_amount, 'price': exit_price,
                                           'cost': exit_cost})
                        shrt_amount[s] -= exit_amount
                        shrt_cost[s] -= exit_cost
                        if shrt_amount[s] <= 0.0 or shrt_cost[s] <= 0.0:
                            shrt_amount[s], shrt_cost[s] = 0.0, 0.0
        else:
            min_exit_cost = default_cost * settings['min_exit_cost_multiplier']
            try:
                long_vwap = long_cost[s] / max(long_amount[s], 9e-9)
            except ZeroDivisionError:
                long_vwap = 0.0
            profit_pct = 1 + min(0.1, max(settings['coins'][coin]['profit_pct'],
                                          -bag_ratio * settings['profit_pct_multiplier']))
            exit_price = max(row.exit_price, round_up(long_vwap * profit_pct, precisions[s]))
            exit_prices_list.append({'timestamp': row.Index, 'symbol': s,
                                     'side': 'sell',
                                     'price': exit_price})
            if coin in coins_long and long_amount[s] * row.exit_price >= min_exit_cost:
                exit_cost = long_amount[s] * exit_price
                if coin in coins_long and row.price > exit_price and exit_cost >= min_exit_cost:
                    coin_avbl_quot = max(0.0, balance[coin]) * exit_price
                    borrow_amount_quot = max(0.0, min(exit_cost - coin_avbl_quot, credit_avbl_quot))
                    exit_cost = min(coin_avbl_quot + borrow_amount_quot, exit_cost)
                    if exit_cost >= min_exit_cost:
                        exit_amount = exit_cost / exit_price
                        balance[coin] -= exit_amount
                        balance[quot] += exit_cost * fee
                        exits_list.append({'symbol': s, 'timestamp': row.Index, 'side': 'sell',
                                           'amount': exit_amount, 'price': exit_price,
                                           'cost': exit_cost})
                        long_amount[s] -= exit_amount
                        long_cost[s] -= exit_cost
                        if long_amount[s] <= 0.0 or long_cost[s] <= 0.0:
                            long_amount[s], long_cost[s] = 0.0, 0.0
    
        acc_equity_quot -= (balance_ito_quot[coin] + balance_ito_quot[quot])
        acc_debt_quot -= -(min(0.0, balance_ito_quot[coin]) + min(0.0, balance_ito_quot[quot]))

        balance_ito_quot[coin] = balance[coin] * row.price
        balance_ito_quot[quot] = balance[quot]

        acc_equity_quot += (balance_ito_quot[coin] + balance_ito_quot[quot])
        acc_debt_quot += -(min(0.0, balance_ito_quot[coin]) + min(0.0, balance_ito_quot[quot]))

        onhand_ito_quot = sum([max(0.0, v) for v in balance_ito_quot.values()])
        margin_level = onhand_ito_quot / acc_debt_quot if acc_debt_quot > 0.0 else 9e9
        if margin_level <= settings['liquidation_margin_level']:
            print('\nliquidation!')
            return balance_list, entries_list, exits_list, exit_prices_list

        k += 1
        if k % 5000 == 0:
            acc_equity_quot = sum(balance_ito_quot.values())
            acc_debt_quot = -sum([min(0.0, v) for v in balance_ito_quot.values()])
            n_millis = row.Index - start_ts
            n_days = n_millis / 1000 / 60 / 60 / 24
            avg_daily_gain = (acc_equity_quot / settings['start_quot'])**(1/n_days)
            bag_ratios = {f'bag_ratio_{s2c[s]}': ((long_amount[s] - shrt_amount[s]) *
                                                  last_price[s]) / acc_equity_quot
                          for s in s2c}
            balance_list.append({**balance_ito_quot,
                                 **{'acc_equity_quot': acc_equity_quot,
                                    'acc_debt_quot': acc_debt_quot,
                                    'onhand_ito_quot': onhand_ito_quot,
                                    'credit_avbl_quot': credit_avbl_quot,
                                    'avg_daily_gain': avg_daily_gain,
                                    'timestamp': row.Index},
                                 **bag_ratios})
            line = f'\r{(n_millis / ts_range) * 100:.2f}% '
            line += f'n_days {n_days:.2f} '
            line += f'acc equity quot: {acc_equity_quot:.6f}  '
            line += f"avg daily gain: {avg_daily_gain:6f} "
            line += f'cost {default_cost:.8f} '
            line += f'credit_avbl_quot {credit_avbl_quot:.6f} '
            line += f'margin_level {margin_level:.4f} '
            sys.stdout.write(line)
            sys.stdout.flush()

    return balance_list, entries_list, exits_list, exit_prices_list


def main():
    pass

if __name__ == '__main__':
    main()
