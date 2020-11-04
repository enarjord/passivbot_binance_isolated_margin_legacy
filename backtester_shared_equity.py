import sys
import pandas as pd
import numpy as np
from common_functions import round_up, round_dn, format_float
from typing import Dict


HOUR_TO_MILLIS = 60 * 60 * 1000
DAY_TO_MILLIS = HOUR_TO_MILLIS * 24


def calc_price_precision(vals: np.ndarray):
    return sorted([len((sn := format_float(round(n, 10)))[sn.find('.') + 1:])
                   for n in vals[::len(vals) // 100]])[98]


def prep_df(ohlcvs: pd.DataFrame, settings: dict) -> pd.DataFrame:
    ema_spans = settings['ema_spans_minutes']
    entry_spread = settings['entry_spread']
    precision = calc_price_precision(ohlcvs.close)
    emas = pd.concat([pd.Series(ohlcvs.close.ewm(span=span, adjust=False).mean(), name=str(span))
                      for span in ema_spans], axis=1)
    min_ema = emas.min(axis=1)
    max_ema = emas.max(axis=1)
    entry_bid = round_dn(min_ema * (1 - entry_spread), precision)
    entry_ask = round_up(max_ema * (1 + entry_spread), precision)
    exit_bid = round_dn(min_ema, precision)
    exit_ask = round_up(max_ema, precision)
    avg = ohlcvs[['open', 'high', 'low', 'close']].mean(axis=1)
    df = pd.DataFrame({'entry_bid': entry_bid, 'entry_ask': entry_ask,
                       'exit_bid': exit_bid, 'exit_ask': exit_ask,
                       'avg': avg, 'high': ohlcvs.high, 'low': ohlcvs.low}, index=ohlcvs.index)
    return df[(df.low < df.exit_bid) | (df.high > df.exit_ask)]


def merge_dfs(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    r = []
    for s in dfs:
        df = dfs[s]
        df.loc[:,'symbol'] = np.repeat(s, len(df))
        r.append(df)
    return pd.concat(r).sort_index()


def backtest(df: pd.DataFrame, settings: dict):

    symbols = settings['symbols']
    precisions = settings['precisions']

    s2c = {s: s.split('/')[0] for s in symbols}
    coins = sorted(set(s2c.values()))
    quot = symbols[0].split('/')[1]
    assert all([s.split('/')[1] == quot for s in symbols])

    # max percentage of total account equity same side entry volume per symbol per hour
    account_equity_pct_per_hour = settings['max_entry_acc_val_pct_per_hour'] / len(symbols)

    # max percentage of total account equity same side entry cost per symbol per entry
    account_equity_pct_per_entry = account_equity_pct_per_hour * settings['min_entry_delay_hours']

    entry_delay_millis = settings['min_entry_delay_hours'] * HOUR_TO_MILLIS

    print('account_equity_pct_per_entry', account_equity_pct_per_entry)
    print('account_equity_pct_per_hour', account_equity_pct_per_hour)
    print('min_entry_delay_hours', settings['min_entry_delay_hours'])

    margin_multiplier = settings['max_leverage'] - 1
    exponent = settings['entry_vol_modifier_exponent']
    min_exit_cost_multiplier = settings['min_exit_cost_multiplier']
    n_days_to_min_markup = settings['n_days_to_min_markup']
    fee = 0.999

    equity = {coin: 0.0 for coin in coins}
    equity[quot] = 1.0
    equity_ito_quot = equity.copy()
    account_equity = equity[quot]
    debt = 0.0
    debt_neg = 0.0
    onhand = equity[quot]

    long_cost = {s: 0.0 for s in symbols}
    long_amount = {s: 0.0 for s in symbols}
    shrt_cost = {s: 0.0 for s in symbols}
    shrt_amount = {s: 0.0 for s in symbols}

    logs = []
    trades = {s: [] for s in symbols}
    prev_entry_ts = {'long': {s: 0 for s in symbols}, 'shrt': {s: 0 for s in symbols}}
    prev_exit_ts = {'long': {s: df.index[0] for s in symbols},
                    'shrt': {s: df.index[0] for s in symbols}}

    last_price = {s: 0.0 for s in symbols}
    for s in symbols:
        for row in df.itertuples():
            if row.symbol == s:
                last_price[s] = row.avg
                break
    last_price[f'{quot}/{quot}'] = 1.0

    start_ts, end_ts = df.index[0], df.index[-1]
    ts_range = end_ts - start_ts
    k = 0

    for row in df.itertuples():
        s = row.symbol
        coin = s2c[s]
        entry_cost = account_equity * account_equity_pct_per_entry
        credit = account_equity * margin_multiplier - debt_neg
        try:
            long_vwap = long_cost[s] / long_amount[s]
        except ZeroDivisionError:
            long_vwap = row.avg
        try:
            shrt_vwap = shrt_cost[s] / shrt_amount[s]
        except ZeroDivisionError:
            shrt_vwap = row.avg

        ##### long exit #####
        long_bag_duration_days = (row.Index - prev_exit_ts['long'][s]) / DAY_TO_MILLIS
        long_exit_markup = max(settings['min_markup_pct'],
                               settings['max_markup_pct'] * (1 - (long_bag_duration_days /
                                                                  n_days_to_min_markup)))
        long_exit_price = max(round_up(long_vwap * (1 + long_exit_markup), precisions[s]),
                              row.exit_ask)
        if row.high > long_exit_price:
            coin_avbl = credit / row.avg + max(0.0, equity[coin])
            long_exit_amount = min(coin_avbl, long_amount[s])
            long_exit_cost = long_exit_amount * long_exit_price
            if long_exit_cost > entry_cost * min_exit_cost_multiplier:
                equity[coin] -= long_exit_amount
                equity[quot] += long_exit_cost * fee
                trades[s].append({'timestamp': row.Index, 'side': 'sel', 'type': 'exit',
                                  'price': long_exit_price,
                                  'amount': long_exit_amount, 'cost': long_exit_cost,
                                  'fee': long_exit_cost * fee})
                if long_exit_amount < long_amount[s]:
                    # partial exit
                    long_cost[s] -= long_exit_cost
                    long_amount[s] -= long_exit_amount
                else:
                    prev_exit_ts['long'][s] = row.Index
                    long_cost[s] = 0.0
                    long_amount[s] = 0.0

        ##### shrt exit #####
        shrt_bag_duration_days = (row.Index - prev_exit_ts['shrt'][s]) / DAY_TO_MILLIS
        shrt_exit_markup = max(settings['min_markup_pct'],
                               settings['max_markup_pct'] * (1 - (shrt_bag_duration_days /
                                                                  n_days_to_min_markup)))
        shrt_exit_price = min(round_dn(shrt_vwap * (1 - shrt_exit_markup), precisions[s]),
                              row.exit_bid)
        if row.low < shrt_exit_price:
            quot_avbl = credit + max(0.0, equity[quot])
            shrt_exit_amount = min(quot_avbl / shrt_exit_price, shrt_amount[s])
            shrt_exit_cost = shrt_exit_amount * shrt_exit_price
            if shrt_exit_cost > entry_cost * min_exit_cost_multiplier:
                equity[quot] -= shrt_exit_cost
                equity[coin] += shrt_amount[s] * fee
                trades[s].append({'timestamp': row.Index, 'side': 'buy', 'type': 'exit',
                                  'price': shrt_exit_price,
                                  'amount': shrt_amount[s], 'cost': shrt_exit_cost,
                                  'fee': shrt_exit_cost * fee})
                if shrt_exit_amount < shrt_amount[s]:
                    # partial exit
                    shrt_cost[s] -= shrt_exit_cost
                    shrt_amount[s] -= shrt_exit_amount
                else:
                    prev_exit_ts['shrt'][s] = row.Index
                    shrt_cost[s] = 0.0
                    shrt_amount[s] = 0.0

        ##### long entry #####
        if row.Index - prev_entry_ts['long'][s] >= entry_delay_millis:
            if row.low < row.entry_bid:
                long_entry_cost = entry_cost * max(1.0, min(min_exit_cost_multiplier / 2,
                                                            (long_vwap / row.entry_bid)**exponent))
                if credit + max(0.0, equity[quot]) >= long_entry_cost:
                    long_entry_amount = long_entry_cost / row.entry_bid
                    equity[quot] -= long_entry_cost
                    equity[coin] += long_entry_amount * fee
                    long_cost[s] += long_entry_cost
                    long_amount[s] += long_entry_amount
                    prev_entry_ts['long'][s] = row.Index
                    trades[s].append({'timestamp': row.Index, 'side': 'buy', 'type': 'entry',
                                      'price': row.entry_bid,
                                      'long_vwap': long_cost[s] / long_amount[s],
                                      'amount': long_entry_amount, 'cost': long_entry_cost,
                                      'fee': long_entry_cost * fee})

        ##### shrt entry #####
        if row.Index - prev_entry_ts['shrt'][s] >= entry_delay_millis:
            if row.high > row.entry_ask:
                shrt_entry_cost = entry_cost * max(1.0, min(min_exit_cost_multiplier / 2,
                                                            (row.entry_ask / shrt_vwap)**exponent))
                if credit + max(0.0, equity[coin] * row.entry_ask) >= shrt_entry_cost:
                    shrt_entry_amount = shrt_entry_cost / row.entry_ask
                    equity[coin] -= shrt_entry_amount
                    equity[quot] += shrt_entry_cost * fee
                    shrt_cost[s] += shrt_entry_cost
                    shrt_amount[s] += shrt_entry_amount
                    prev_entry_ts['shrt'][s] = row.Index
                    trades[s].append({'timestamp': row.Index, 'side': 'sel', 'type': 'entry',
                                      'price': row.entry_ask,
                                      'shrt_vwap': shrt_cost[s] / shrt_amount[s],
                                      'amount': shrt_entry_amount, 'cost': shrt_entry_cost,
                                      'fee': shrt_entry_cost * fee})

        account_equity -= (equity_ito_quot[coin] + equity_ito_quot[quot])
        debt -= (min(0.0, equity_ito_quot[coin]) + min(0.0, equity_ito_quot[quot]))
        onhand -= (max(0.0, equity_ito_quot[coin]) + max(0.0, equity_ito_quot[quot]))
        equity_ito_quot[s2c[s]] = equity[s2c[s]] * row.avg
        equity_ito_quot[quot] = equity[quot]
        account_equity += (equity_ito_quot[coin] + equity_ito_quot[quot])
        debt += (min(0.0, equity_ito_quot[coin]) + min(0.0, equity_ito_quot[quot]))
        onhand += (max(0.0, equity_ito_quot[coin]) + max(0.0, equity_ito_quot[quot]))
        debt_neg = round(max(0.0, debt * -1), 4)
        margin_level = min(5.0, onhand / debt_neg if debt_neg else 5.0)
        if margin_level < 1.05:
            print('\nliquidation!')
            break

        k += 1
        if k % 1000 == 0:
            log_entry = {**{'timestamp': row.Index, 'debt': debt_neg, 'onhand': onhand,
                            'credit': credit, 'margin_level': margin_level,
                            'equity': account_equity},
                         **equity_ito_quot}
            logs.append(log_entry)
            n_millis = row.Index - start_ts
            n_days = n_millis / (1000 * 60 * 60 * 24)
            line = f'{n_millis / ts_range:.2f} margin_level, {margin_level:.2f}'
            line += f' equity {account_equity:.4f} credit {credit:4f}'
            sys.stdout.write('\r' + line + ' ' * 8)
    return logs, trades

