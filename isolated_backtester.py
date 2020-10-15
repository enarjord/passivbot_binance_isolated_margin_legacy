import sys
import pandas as pd
import numpy as np
from common_functions import round_up, round_dn, format_float


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
    return df


def backtest(df, settings: dict):

    start_equity = 1.0

    balance = [start_equity, 0.0]

    equity = start_equity
    coin_ito_quot = 0.0
    debt = 0.0

    precision = calc_price_precision(df.low)

    long_cost, long_amount = 0.0, 0.0
    shrt_cost, shrt_amount = 0.0, 0.0
    sum_cost, sum_amount = 0.0, 0.0
    long_vwap = shrt_vwap = sum_vwap = df.iloc[0].avg

    prev_long_entry_ts, prev_shrt_entry_ts = 0, 0
    prev_long_exit_ts = prev_shrt_exit_ts = df.index[0]

    logs = []
    trades = []

    margin = settings['max_leverage'] - 1
    min_exit_cost_multiplier = settings['min_exit_cost_multiplier']
    entry_vol_modifier_exponent = settings['entry_vol_modifier_exponent']
    n_days_dim = settings['n_days_to_min_markup']

    fee = 0.001
    fee_m = 1 - fee

    start_ts, end_ts = df.index[0], df.index[-1]
    ts_range = end_ts - start_ts
    k = 0

    for row in df.itertuples():
        credit_avbl = max(0.0, equity * margin - debt)
        entry_cost = equity * settings['account_equity_pct_per_entry']
        delay_hours = entry_cost / settings['account_equity_pct_per_hour'] * equity
        if max(0.0, balance[0]) + credit_avbl > entry_cost and row.low < row.entry_bid and \
                row.Index - prev_long_entry_ts >= delay_hours * HOUR_TO_MILLIS:
            long_entry_cost = entry_cost * min(min_exit_cost_multiplier / 2,
                                               (long_vwap / row.avg) ** entry_vol_modifier_exponent)
            balance[0] -= long_entry_cost
            long_entry_amount = long_entry_cost / row.entry_bid
            balance[1] += long_entry_amount * fee_m
            long_cost += long_entry_cost
            long_amount += long_entry_amount
            sum_cost += long_entry_cost
            sum_amount += long_entry_amount
            prev_long_entry_ts = row.Index
            trades.append({'side': 'buy', 'amount': long_entry_amount, 'price': row.entry_bid,
                           'cost': long_entry_cost,
                           'fee': long_entry_amount * fee, 'quot_fee': False, 'type': 'entry',
                           'timestamp': row.Index})
        if max(0.0, coin_ito_quot) + credit_avbl > entry_cost and \
                row.high > row.entry_ask and \
                row.Index - prev_shrt_entry_ts >= delay_hours * HOUR_TO_MILLIS:
            shrt_entry_cost = entry_cost * min(min_exit_cost_multiplier / 2,
                                    (row.avg / shrt_vwap) ** entry_vol_modifier_exponent)
            shrt_entry_amount = shrt_entry_cost / row.entry_ask
            balance[1] -= shrt_entry_amount
            balance[0] += shrt_entry_cost * fee_m
            shrt_cost += shrt_entry_cost
            shrt_amount += shrt_entry_amount
            sum_cost += shrt_entry_cost
            sum_amount += shrt_entry_amount
            prev_shrt_entry_ts = row.Index
            trades.append({'side': 'sel', 'amount': shrt_entry_amount, 'price': row.entry_ask,
                           'cost': shrt_entry_cost,
                           'fee': entry_cost * fee, 'quot_fee': True, 'type': 'entry',
                           'timestamp': row.Index})

        sum_vwap = max(min(sum_cost / sum_amount if sum_amount > 0.0 else row.avg, row.avg * 1.3),
                       row.avg * 0.7)

        long_vwap = max(min(long_cost / long_amount if long_amount else row.avg, row.avg * 1.3),
                        row.avg * 0.7)
        long_bag_duration_days = (row.Index - prev_long_exit_ts) / DAY_TO_MILLIS
        long_vwap_multiplier = 1 + max(
            settings['min_markup_pct'],
            ((n_days_dim - long_bag_duration_days) / n_days_dim) * settings['max_markup_pct']
        )
        long_exit_price = max(row.exit_ask, round_up(long_vwap_multiplier * long_vwap, precision))
        if long_amount and row.high > long_exit_price:
            coin_ito_quot_avbl = max(0.0, balance[1] / row.avg) + credit_avbl
            long_exit_cost = min(coin_ito_quot_avbl, long_amount * long_exit_price)
            long_exit_amount = long_exit_cost / long_exit_price
            if long_exit_cost > entry_cost * min_exit_cost_multiplier:
                balance[1] -= long_exit_amount
                balance[0] += long_exit_cost * fee_m
                long_cost -= long_exit_cost
                long_amount -= long_exit_amount
                if long_cost <= entry_cost or long_amount <= entry_cost / long_exit_price:
                    long_cost, long_amount = 0.0, 0.0
                    prev_long_exit_ts = row.Index
                else:
                    print('partial long exit, cost left', long_cost)
                trades.append({'side': 'sel', 'amount': long_exit_amount, 'price': long_exit_price,
                               'cost': long_exit_cost, 'fee': long_exit_cost * fee,
                               'quot_fee': True, 'type': 'exit', 'timestamp': row.Index})

        shrt_vwap = max(min(shrt_cost / shrt_amount if shrt_amount else row.avg, row.avg * 1.3),
                        row.avg * 0.7)
        shrt_bag_duration_days = (row.Index - prev_shrt_exit_ts) / DAY_TO_MILLIS
        shrt_vwap_multiplier = 1 - max(
            settings['min_markup_pct'],
            ((n_days_dim - shrt_bag_duration_days) / n_days_dim) * settings['max_markup_pct']
        )
        shrt_exit_price = min(round_dn(shrt_vwap * shrt_vwap_multiplier, precision), row.exit_bid)
        #shrt_exit_price = round_dn(shrt_vwap * shrt_vwap_multiplier, precision)
        if shrt_amount and row.low < shrt_exit_price:
            quot_avbl = max(0.0, balance[0]) + credit_avbl
            shrt_exit_cost = min(shrt_amount * shrt_exit_price, quot_avbl)
            shrt_exit_amount = shrt_exit_cost / shrt_exit_price
            if shrt_exit_cost > entry_cost * min_exit_cost_multiplier:
                balance[0] -= shrt_exit_cost
                balance[1] += shrt_exit_amount * fee_m
                shrt_cost -= shrt_exit_cost
                shrt_amount -= shrt_exit_amount
                if shrt_cost <= entry_cost or shrt_amount <= entry_cost / shrt_exit_price:
                    shrt_cost, shrt_amount = 0.0, 0.0
                    prev_shrt_exit_ts = row.Index
                else:
                    print('partial shrt exit, cost left', shrt_cost)
                trades.append({'side': 'buy', 'amount': shrt_exit_amount, 'price': shrt_exit_price,
                               'cost': shrt_exit_cost, 'fee': shrt_exit_amount * fee,
                               'quot_fee': False, 'type': 'exit', 'timestamp': row.Index})
        coin_ito_quot = balance[1] * row.avg
        equity = balance[0] + coin_ito_quot
        total_asset_value = max(0.0, balance[0]) + max(0.0, coin_ito_quot)
        debt = max(0.0, (min(0.0, balance[0]) + min(0.0, coin_ito_quot)) * -1)
        margin_level = min(total_asset_value / debt if debt else 1.5, 1.5)
        logs.append({'equity': equity, 'quot': balance[0], 'coin': balance[1],
                     'coin_ito_quot': coin_ito_quot,
                     'margin_level': margin_level, 'debt': debt, 'credit_avbl': credit_avbl,
                     'long_vwap': long_exit_price, 'shrt_vwap': shrt_exit_price,
                     'sum_vwap': sum_vwap,
                     'timestamp': row.Index})
        k += 1
        if k % 5000 == 0:
            n_millis = row.Index - start_ts
            n_days = n_millis / (1000 * 60 * 60 * 24)
            adg = (equity / start_equity)**(1/n_days)
            line = f'{n_millis / ts_range:.2f} adg {adg:.4f} margin_level {margin_level:.4f}'
            sys.stdout.write('\r' + line)
        if logs[-1]['margin_level'] < 1.05:
            print('\nliquidation!')
            break
    return logs, trades
