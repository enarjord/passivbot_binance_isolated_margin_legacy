import trade_data
import pandas as pd
import numpy as np
import sys
from common_functions import round_up, round_dn


def get_cutoff_index(lst: [dict], age_limit: int) -> int:
    for i in range(len(lst)):
        if lst[i]['timestamp'] >= age_limit:
            return i
    return len(lst)


def backtest(df: pd.DataFrame, settings: dict, price_precisions: dict = {}):
    start_quot = 1.0
    if 'exponent' not in settings:
        settings['exponent'] = 15
    ppctminus = 1 - settings['profit_pct']
    ppctplus = 1 + settings['profit_pct']
    symbols = [c.replace('_low', '') for c in df.columns if 'low' in c]
    if not price_precisions:
        price_precisions = {s: 8 for s in symbols}
    lows = {s: f'{s}_low' for s in symbols}
    highs = {s: f'{s}_high' for s in symbols}
    means = {s: f'{s}_mean' for s in symbols}

    account_equity_pct_per_period = settings['account_equity_pct_per_hour'] * \
        settings['hours_rolling_small_trade_window']

    min_emas = {s: f'{s}_mean_min_ema' for s in symbols}
    max_emas = {s: f'{s}_mean_max_ema' for s in symbols}

    rolling_millis = settings['max_memory_span_days'] * 24 * 60 * 60 * 1000
    rolling_trade_window_millis = settings['hours_rolling_small_trade_window'] * 60 * 60 * 1000

    s2c = {s: s.split('_')[0] for s in symbols}
    quot = symbols[0].split('_')[1]
    balance = {s2c[s]: 0.0 for s in s2c}
    balance[quot] = 1.0
    acc_equity_quot = 1.0
    acc_debt_quot = 0.0
    long_entries = {s: [] for s in symbols}
    shrt_entries = {s: [] for s in symbols}
    long_exits = {s: [] for s in symbols}
    shrt_exits = {s: [] for s in symbols}
    long_exit_price_list = {s: [] for s in symbols}
    shrt_exit_price_list = {s: [] for s in symbols}

    past_rolling_long_entries = {s: [] for s in symbols}
    past_rolling_shrt_entries = {s: [] for s in symbols}

    past_n_hours_long_entries = {s: [] for s in symbols}
    past_n_hours_shrt_entries = {s: [] for s in symbols}
    past_n_hours_long_cost = {s: 0.0 for s in symbols}
    past_n_hours_shrt_cost = {s: 0.0 for s in symbols}


    entry_bid = {s: round(df.iloc[0][means[s]], 8) for s in symbols}
    entry_ask = {s: round(df.iloc[0][means[s]], 8) for s in symbols}

    exit_bid = {s: entry_bid[s] for s in symbols}
    exit_ask = {s: entry_ask[s] for s in symbols}

    long_cost = {s: 0.0 for s in symbols}
    long_amount = {s: 0.0 for s in symbols}
    shrt_cost = {s: 0.0 for s in symbols}
    shrt_amount = {s: 0.0 for s in symbols}

    fee = 1 - 0.000675 # vip 1

    margin_level = 3 - 1

    balance_list = []


    start_ts, end_ts = df.index[0], df.index[-1]
    ts_range = end_ts - start_ts

    for row in df.itertuples():
        cost = acc_equity_quot * settings['account_equity_pct_per_trade']
        min_exit_cost = cost * 6
        credit_avbl_quot = max(0.0, acc_equity_quot * margin_level - acc_debt_quot)
        age_limit = row.Index - rolling_millis
        trade_window_age_limit = row.Index - rolling_trade_window_millis
        for s in symbols:
            # rolling longs
            long_i = get_cutoff_index(past_rolling_long_entries[s], age_limit)
            if long_i > 0:
                slc = past_rolling_long_entries[s][:long_i]
                past_rolling_long_entries[s] = past_rolling_long_entries[s][long_i:]
                long_amount[s] -= sum([e['amount'] for e in slc])
                long_cost[s] -= sum([e['amount'] * e['price'] for e in slc])
                if long_cost[s] <= 0.0 or long_amount[s] <= 0.0:
                    long_cost[s] = 0.0
                    long_amount[s] = 0.0
                    past_rolling_long_entries[s] = []
                    exit_ask[s] = getattr(row, means[s])
                else:
                    exit_ask[s] = (long_cost[s] / long_amount[s]) * ppctplus

            # rolling shrts
            shrt_i = get_cutoff_index(past_rolling_shrt_entries[s], age_limit)
            if shrt_i > 0:
                slc = past_rolling_shrt_entries[s][:shrt_i]
                past_rolling_shrt_entries[s] = past_rolling_shrt_entries[s][shrt_i:]
                shrt_cost[s] -= sum([e['amount'] * e['price'] for e in slc])
                shrt_amount[s] -= sum([e['amount'] for e in slc])
                if shrt_cost[s] <= 0.0 or shrt_amount[s] <= 0.0:
                    shrt_cost[s] = 0.0
                    shrt_amount[s] = 0.0
                    past_rolling_shrt_entries[s] = []
                    exit_bid[s] = getattr(row, means[s])
                else:
                    exit_bid[s] = (shrt_cost[s] / shrt_amount[s]) * ppctminus

            if getattr(row, lows[s]) < entry_bid[s]:
                li = get_cutoff_index(past_n_hours_long_entries[s], trade_window_age_limit)
                if li > 0:
                    slc = past_n_hours_long_entries[s][:li]
                    past_n_hours_long_entries[s] = past_n_hours_long_entries[s][li:]
                    past_n_hours_long_cost[s] -= sum([e['price'] * e['amount'] for e in slc])
                # long buy
                long_modifier = max(
                    1.0, min(5.0, (exit_ask[s] / getattr(row, means[s]))**settings['exponent']))
                if acc_equity_quot * account_equity_pct_per_period * long_modifier > \
                        past_n_hours_long_cost[s]:
                    buy_cost = cost * long_modifier
                    if balance[quot] >= buy_cost:
                        # long buy normal
                        buy_amount = (buy_cost / entry_bid[s])
                        balance[quot] -= buy_cost
                        balance[s2c[s]] += buy_amount * fee
                        long_entries[s].append({'price': entry_bid[s], 'amount': buy_amount,
                                                'timestamp': row.Index})
                        past_rolling_long_entries[s].append(long_entries[s][-1])
                        past_n_hours_long_entries[s].append(long_entries[s][-1])
                        past_n_hours_long_cost[s] += buy_cost
                        long_amount[s] += buy_amount
                        long_cost[s] += buy_cost
                        exit_ask[s] = (long_cost[s] / long_amount[s]) * ppctplus
                    elif credit_avbl_quot > 0.0:
                        # long buy with credit
                        quot_avbl = max(0.0, balance[quot])
                        to_borrow = min(credit_avbl_quot, buy_cost - quot_avbl)
                        credit_avbl_quot -= to_borrow
                        partial_buy_cost = quot_avbl + to_borrow
                        buy_amount = (partial_buy_cost / entry_bid[s])
                        balance[quot] -= partial_buy_cost
                        balance[s2c[s]] += buy_amount * fee
                        long_entries[s].append({'price': entry_bid[s], 'amount': buy_amount,
                            'timestamp': row.Index})
                        past_rolling_long_entries[s].append(long_entries[s][-1])
                        past_n_hours_long_entries[s].append(long_entries[s][-1])
                        past_n_hours_long_cost[s] += (long_entries[s][-1]['price'] *
                                                      long_entries[s][-1]['amount'])
                        long_amount[s] += buy_amount
                        long_cost[s] += partial_buy_cost
                        exit_ask[s] = (long_cost[s] / long_amount[s]) * ppctplus
            if getattr(row, highs[s]) > entry_ask[s]:
                si = get_cutoff_index(past_n_hours_shrt_entries[s], trade_window_age_limit)
                if si > 0:
                    slc = past_n_hours_shrt_entries[s][:si]
                    past_n_hours_shrt_entries[s] = past_n_hours_shrt_entries[s][si:]
                    past_n_hours_shrt_cost[s] -= sum([e['price'] * e['amount'] for e in slc])
                # shrt sel
                shrt_modifier = max(
                    1.0, min(5.0, (getattr(row, means[s]) / exit_bid[s])**settings['exponent']))
                if acc_equity_quot * account_equity_pct_per_period * shrt_modifier > \
                        past_n_hours_shrt_cost[s]:
                    sel_cost = cost * shrt_modifier
                    sel_amount = sel_cost / entry_ask[s]
                    if balance[s2c[s]] >= sel_amount:
                        # shrt sel normal
                        balance[s2c[s]] -= sel_amount
                        balance[quot] += sel_cost * fee
                        shrt_entries[s].append({'price': entry_ask[s], 'amount': sel_amount,
                                                'timestamp': row.Index})
                        past_rolling_shrt_entries[s].append(shrt_entries[s][-1])
                        past_n_hours_shrt_entries[s].append(shrt_entries[s][-1])
                        past_n_hours_shrt_cost[s] += sel_cost
                        shrt_amount[s] += sel_amount
                        shrt_cost[s] += sel_cost
                        exit_bid[s] = (shrt_cost[s] / shrt_amount[s]) * ppctminus
                    elif credit_avbl_quot > 0.0:
                        # shrt sel with credit
                        coin_avbl = max(0.0, balance[s2c[s]])
                        to_borrow = min(credit_avbl_quot / entry_ask[s], sel_amount - coin_avbl)
                        credit_avbl_quot -= (to_borrow * entry_ask[s])
                        partial_sel_amount = coin_avbl + to_borrow
                        balance[s2c[s]] -= partial_sel_amount
                        partial_sel_cost = partial_sel_amount * entry_ask[s]
                        balance[quot] += partial_sel_cost * fee
                        shrt_entries[s].append({'price': entry_ask[s], 'amount': partial_sel_amount,
                                                'timestamp': row.Index})
                        past_rolling_shrt_entries[s].append(shrt_entries[s][-1])
                        past_n_hours_shrt_entries[s].append(shrt_entries[s][-1])
                        past_n_hours_shrt_cost[s] += partial_sel_cost
                        shrt_amount[s] += partial_sel_amount
                        shrt_cost[s] += partial_sel_cost
                        exit_bid[s] = (shrt_cost[s] / shrt_amount[s]) * ppctminus

            exit_ask[s] = round_up(max(exit_ask[s], entry_ask[s]), price_precisions[s])
            exit_bid[s] = round_dn(min(exit_bid[s], entry_bid[s]), price_precisions[s])

            if long_cost[s] > min_exit_cost:
                # long sel
                long_exit_price_list[s].append({'price': exit_ask[s], 'timestamp': row.Index})
                if getattr(row, highs[s]) > exit_ask[s]:
                    if balance[s2c[s]] >= long_amount[s]:
                        # long sel normal
                        long_sel_amount = max(balance[s2c[s]], long_amount[s])
                        long_exits[s].append({'price': exit_ask[s], 'amount': long_sel_amount,
                                              'timestamp': row.Index})
                        quot_acquired = long_sel_amount * exit_ask[s]
                        balance[s2c[s]] -= long_sel_amount
                        balance[quot] += quot_acquired * fee
                        long_amount[s] = 0.0
                        long_cost[s] = 0.0
                    else:
                        # partial long sel
                        coin_avbl = max(0.0, balance[s2c[s]])
                        to_borrow = min(credit_avbl_quot / exit_ask[s], long_amount[s] - coin_avbl)
                        partial_sel_amount = coin_avbl + to_borrow
                        if partial_sel_amount > 0.0:
                            credit_avbl_quot -= (to_borrow * exit_ask[s])
                            balance[s2c[s]] -= partial_sel_amount
                            partial_sel_cost = partial_sel_amount * exit_ask[s]
                            balance[quot] += partial_sel_cost * fee
                            long_exits[s].append({'price': exit_ask[s],
                                                  'amount': partial_sel_amount,
                                                  'timestamp': row.Index})
                            long_amount[s] -= partial_sel_amount
                            long_cost[s] -= partial_sel_cost
                    if long_amount[s] <= 0.0 or long_cost[s] <= 0.0:
                        long_amount[s] = 0.0
                        long_cost[s] = 0.0
                        past_rolling_long_entries[s] = []
                        past_n_hours_long_entries[s] = []
                        past_n_hours_long_cost[s] = 0.0
            if shrt_cost[s] > min_exit_cost:
                shrt_exit_price_list[s].append({'price': exit_bid[s], 'timestamp': row.Index})
                if getattr(row, lows[s]) < exit_bid[s]:
                    # shrt buy
                    shrt_buy_cost = shrt_amount[s] * exit_bid[s]
                    if balance[quot] >= shrt_buy_cost:
                        # shrt buy normal
                        shrt_buy_cost = max(shrt_buy_cost,
                                            min(balance[quot], -balance[s2c[s]] * exit_bid[s]))
                        shrt_buy_amount = shrt_buy_cost / exit_bid[s]
                        shrt_exits[s].append({'price': exit_bid[s], 'amount': shrt_buy_amount,
                                              'timestamp': row.Index})
                        balance[quot] -= shrt_buy_cost
                        balance[s2c[s]] += shrt_buy_amount * fee
                        shrt_amount[s] = 0.0
                        shrt_cost[s] = 0.0
                    else:
                        # partial shrt buy
                        quot_avbl = max(0.0, balance[quot])
                        to_borrow = min(credit_avbl_quot, shrt_buy_cost - quot_avbl)
                        partial_sel_cost = quot_avbl + to_borrow
                        if partial_sel_cost > 0.0:
                            coin_acquired = partial_sel_cost / exit_bid[s]
                            shrt_exits[s].append({'price': exit_bid[s], 'amount': coin_acquired,
                                                  'timestamp': row.Index})
                            credit_avbl_quot -= to_borrow
                            balance[quot] -= partial_sel_cost
                            balance[s2c[s]] += coin_acquired * fee
                            shrt_amount[s] -= coin_acquired
                            shrt_cost[s] -= partial_sel_cost
                    if shrt_amount[s] <= 0.0 or shrt_cost[s] <= 0.0:
                        shrt_amount[s] = 0.0
                        shrt_cost[s] = 0.0
                        past_rolling_shrt_entries[s] = []
                        past_n_hours_shrt_entries[s] = []
                        past_n_hours_shrt_cost[s] = 0.0

            entry_bid[s] = round_dn(
                min(getattr(row, means[s]), getattr(row, min_emas[s])), price_precisions[s])
            entry_ask[s] = round_up(
                max(getattr(row, means[s]), getattr(row, max_emas[s])), price_precisions[s])

        acc_equity_quot = \
            balance[quot] + sum([balance[s2c[s]] * getattr(row, means[s]) for s in symbols])
        balance_list.append({**{s2c[s]: balance[s2c[s]] * getattr(row, means[s]) for s in symbols},
                             **{'acc_equity_quot': acc_equity_quot, 'timestamp': row.Index,
                                quot: balance[quot]}})
        acc_debt_quot = -sum([balance_list[-1][c] for c in balance if balance_list[-1][c] < 0.0])
        balance_list[-1]['acc_debt_quot'] = acc_debt_quot
        if row.Index % 86400000 == 0 or row.Index >= end_ts:
            n_millis = row.Index - start_ts
            line = f'\r{(n_millis / ts_range) * 100:.2f}% '
            line += f'acc equity quot: {acc_equity_quot:.6f}  '
            n_days = n_millis / 1000 / 60 / 60 / 24
            line += f'avg daily gain: {acc_equity_quot**(1/n_days):6f} '
            sys.stdout.write(line)
            sys.stdout.flush()
    return balance_list, long_entries, shrt_entries, long_exits, shrt_exits, \
        long_exit_price_list, shrt_exit_price_list


def load_hlms(symbols: [str], n_days: int, no_download: bool = False) -> pd.DataFrame:
    hlms = []
    for s in symbols:
        ohlcv = trade_data.fetch_ohlcvs(s, n_days, no_download=no_download)
        ohlcv = ohlcv[ohlcv.index > ohlcv.index[-1] - 1000 * 60 * 60 * 24 * n_days]
        hlm = ohlcv[['high', 'low']].join(
            pd.Series(ohlcv[['open', 'high', 'low', 'close']].mean(axis=1), name='mean'))
        hlm.columns = [f"{s.replace('/', '_')}_{c}" for c in hlm.columns]
        hlms.append(hlm)
    return pd.concat(hlms, axis=1).round(10)


def add_emas(hlms: pd.DataFrame, ema_spans):
    min_maxs = []
    for c in filter(lambda x: 'mean' in x, hlms.columns):
        emas = []
        for span in ema_spans:
            ema = hlms[c].ewm(span=span, adjust=False).mean()
            ema.name = str(span)
            emas.append(ema)
        minema = pd.concat(emas, axis=1).min(axis=1)
        minema.name = f'{c}_min_ema'
        maxema = pd.concat(emas, axis=1).max(axis=1)
        maxema.name = f'{c}_max_ema'
        min_maxs.append(pd.concat([minema, maxema], axis=1))
    df = hlms.join(pd.concat(min_maxs, axis=1))
    return custom_fillna(df[sorted(df.columns)])


def custom_fillna(df: pd.DataFrame) -> pd.DataFrame:
    symbols_ = [c.replace('_low', '') for c in df.columns if 'low' in c]
    for s in symbols_:
        clmns = [c for c in df.columns if s in c and not c.endswith('mean')]
        mean_c = s + '_mean'
        df[mean_c] = df[mean_c].fillna(method='ffill').fillna(method='bfill')
        for c in clmns:
            df[c] = df[c].fillna(df[mean_c])
    return df
    


def pseudo_trades(pstart, prange, ts_start, ts_step, p=0.1, r=8):
    n = 100000
    timestamps = list(np.arange(ts_start, ts_start + n * ts_step, ts_step))
    opens = [pstart, pstart]
    for i in range(n-len(opens)):
        opens.append(opens[-1] * (1 + (np.random.random() - 0.5) * p * prange)) 
    opens = np.array(opens)
    highs = np.round(np.array(opens + np.random.random() * prange * p), r)
    lows = np.round(np.array(opens - np.random.random() * prange * p), r)
    closes = np.round(opens + (highs - lows) * (np.random.random() - 0.5), r)
    return pd.DataFrame({'open': opens, 'high': highs, 'low': lows, 'close': closes},
                         index=timestamps)


def main():
    from passivbot import load_settings
    settings = load_settings('default')
    fee = 1 - 0.0675 * 0.01 # vip1

    settings['hours_rolling_small_trade_window'] = 1.0
    settings['account_equity_pct_per_trade'] = 0.0002
    settings['exponent'] = 15


    symbols = [f'{c}/BTC' for c in settings['coins_long']]
    symbols = sorted(symbols)
    n_days = 30 * 4
    #symbols = [s for s in symbols if not any(s.startswith(c) for c in ['VET', 'IOST'])]
    print('loading ohlcvs')
    high_low_means = load_hlms(symbols, n_days, no_download=True)
    print('adding emas')
    df = add_emas(high_low_means, settings['ema_spans_minutes'])


    results = []
    for aepph in list(np.linspace(0.002, 0.006, 5).round(5)):
        for mmsd in list(np.linspace(21, 90, 2).round().astype(int)):
            print('testing', aepph, mmsd)
            settings['account_equity_pct_per_hour'] = aepph
            settings['max_memory_span_days'] = mmsd

            balance_list, lentr, sentr, lexit, sexit, lexitpl, sexitpl = backtest(df, settings)
            bldf = pd.DataFrame(balance_list).set_index('timestamp')
            start_equity = bldf.acc_equity_quot.iloc[0]
            end_equity = bldf.acc_equity_quot.iloc[-1]
            n_days = (bldf.index[-1] - bldf.index[0]) / 1000 / 60 / 60 / 24
            avg_daily_gain = (end_equity / start_equity)**(1 / n_days)
            print()
            print(aepph, mmsd, 'average daily gain', round(avg_daily_gain, 8))
            print(aepph, mmsd, '    low water mark', bldf.acc_equity_quot.min())
            print(aepph, mmsd, '   high water mark', bldf.acc_equity_quot.max())
            print(aepph, mmsd, '              mean', bldf.acc_equity_quot.mean())
            print(aepph, mmsd, '               end', bldf.acc_equity_quot.iloc[-1])

            print('\n\n')

            results.append({
                'account_equity_pct_per_hour': aepph,
                'max_memory_span_days': mmsd,
                'average daily gain': avg_daily_gain,
                'low water mark': bldf.acc_equity_quot.min(),
                'high water mark': bldf.acc_equity_quot.max(),
                'mean': bldf.acc_equity_quot.mean(),
                'end': bldf.acc_equity_quot.iloc[-1],
            })
    for r in sorted(results, key=lambda x: x['mean']):
        print(r)


if __name__ == '__main__':
    main()
