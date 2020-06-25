import datetime
import pandas as pd
import numpy as np

from typing import Callable, Iterator, Tuple

from common_procedures import load_key_secret, make_get_filepath
from common_functions import ts_to_date, ts_to_day

import hmac
import hashlib

from urllib.parse import urljoin, urlencode

import os
import sys
from time import time
import requests
import json


def raw_trades_to_ohlcv(raw_trades: pd.DataFrame, timeframe: int = 1000) -> pd.DataFrame:
    '''
    timeframe in milliseconds
    '''
    volumes = pd.DataFrame({
        'volume_sold': raw_trades['amount'].loc[raw_trades['is_buyer_maker']],
        'volume_bought': raw_trades['amount'].loc[~raw_trades['is_buyer_maker']]
    }, index=raw_trades.index).fillna(0.0)
    df = raw_trades.drop(['is_buyer_maker', 'amount'], axis=1).join(volumes).set_index('timestamp')
    df.index = df.index // timeframe * timeframe
    grouped = df.groupby(df.index)
    new_timestamps = np.arange(
        df.index[0], df.index[-1]+1, timeframe, dtype=int)
    ohlcvs = pd.DataFrame({
        'open': grouped['price'].first(),
        'high': grouped['price'].max(),
        'low': grouped['price'].min(),
        'close': grouped['price'].last(),
        'volume_sold': grouped['volume_sold'].sum(),
        'volume_bought': grouped['volume_bought'].sum(),
        }, index=new_timestamps)
    ohlcvs.loc[:, 'volume_sold'] = ohlcvs['volume_sold'].fillna(0.0)
    ohlcvs.loc[:, 'volume_bought'] = ohlcvs['volume_bought'].fillna(0.0)
    ohlcvs.loc[:, 'volume'] = ohlcvs['volume_bought'] + ohlcvs['volume_sold']
    ohlcvs.loc[:, 'close'] = ohlcvs['close'].fillna(method='ffill')
    ohlcvs.loc[:, 'open'] = ohlcvs['open'].fillna(ohlcvs['close'])
    ohlcvs.loc[:, 'high'] = ohlcvs['high'].fillna(ohlcvs['close'])
    ohlcvs.loc[:, 'low'] = ohlcvs['low'].fillna(ohlcvs['close'])
    ohlcvs = ohlcvs[['open', 'high', 'low', 'close', 'volume', 'volume_bought', 'volume_sold']]
    return ohlcvs


def format_ts_to_backtesting_app(ohlcvs: pd.DataFrame) -> pd.DataFrame:
    datetimes = pd.Series(pd.to_datetime(ohlcvs.index, unit='ms'))
    return pd.DataFrame({**{'date': datetimes.apply(lambda x: x.date()),
                            'time': datetimes.apply(lambda x: x.time())},
                         **{k: ohlcvs[k].values for k in ['open', 'high', 'low', 'close', 'volume',
                                                          'volume_bought', 'volume_sold']}})


def sort_and_drop_duplicates_by_index(df1: pd.DataFrame, df2: pd.DataFrame = None) -> pd.DataFrame:
    if df1 is None and df2 is None:
        return None
    df = pd.concat([df1, df2])
    df = df.sort_index()
    return df.loc[~df.index.duplicated()]


'''
procedures, i.e. functions with side effects
'''


def print_(args, r=False) -> str:
    line = ts_to_date(time())[:19] + '  '
    str_args = '{} ' * len(args)
    line += str_args.format(*args)
    if r:
        sys.stdout.write('\r' + line + '   ')
    else:
        print(line)
    sys.stdout.flush()
    return line


def get_filenames(path: str, required_suffix: str = '.csv') -> list:
    if not os.path.isdir(path):
        return []
    return sorted([f for f in os.listdir(path) if f.endswith(required_suffix)])


def begin_csv(filepath: str, columns: [str]) -> str:
    filepath = make_get_filepath(filepath)
    header = ','.join(columns)
    if os.path.isfile(filepath):
        with open(filepath) as f:
            for line in f:
                first_line = line.strip()
                break
        if first_line != header:
            raise Exception('{} is missing header. Firstline: {}, correct_header: {}'.format(
                filepath, first_line, header))
    else:
        with open(filepath, 'w') as f:
            f.write(header + '\n')
    return filepath


def get_first_and_last_line_in_csv_dir(path: str) -> tuple:
    if not os.path.isdir(path):
        return None, None
    filenames = get_filenames(path)
    if len(filenames) == 0:
        return None, None
    with open(path + filenames[0]) as f:
        for i, line in enumerate(f):
            if i >= 1:
                first = [e.strip() for e in line.split(',')]
                break
    with open(path + filenames[-1]) as f:
        last = [e.strip() for e in f.readlines()[-1].split(',')]
    return first, last


def write_to_cache(items: list, columns: [str], cache_path: str) -> None:
    with open(cache_path, 'a') as f:
        for item in items:
            line = ','.join([str(item[k]) for k in columns]) + '\n'
            f.write(line)


def clear_cache(cache_path: str,
                dirpath: str,
                columns: [str],
                month: bool,
                formatting_func: Callable[[dict], dict]) -> None:
    if not os.path.isfile(cache_path):
        return
    df = pd.read_csv(cache_path)
    if len(df) == 0:
        return
    if month:
        time_period = 'month'
        df.loc[:, time_period] = \
            pd.to_datetime(df['timestamp'], unit='ms').astype(str).apply(lambda x: x[:7])
    else:
        time_period = 'day'
        df.loc[:, time_period] = pd.to_datetime(df['timestamp'], unit='ms').dt.normalize()
    for g in df.groupby(time_period):
        filename = '{}.csv'.format(str(g[0])[:10])
        items = g[1][columns]
        if os.path.isfile(dirpath + filename):
            print_(['{} already present, merging...'.format(filename)])
            to_merge = pd.read_csv(dirpath + filename)
        else:
            print_(['writing {}'.format(filename)])
            to_merge = None
        sort_and_drop_duplicates_by_index(
            *map(formatting_func,
                 [items, to_merge])).to_csv(dirpath + filename)
    os.remove(cache_path)


def fetch_ohlcvs(symbol: str,
                 n_days: float = 7,
                 timeframe: str = '1m',
                 no_download: bool = False) -> pd.DataFrame:
    '''
    fetches ohlcv data from binance
    allowed timeframes are
    1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M
    '''
    def request_klines(start_time: int = -1) -> dict:
        url = 'https://api.binance.com/api/v3/klines?symbol={}&limit=1000&interval={}'.format(
            symbol.replace('/', ''), timeframe, start_time)
        if start_time != -1:
            url += '&startTime={}'.format(start_time)
        return json.loads(requests.get(url).text)

    def format_ohlcvs(ohlcvs_: [dict]) -> [dict]:
        formatted = [{'timestamp': e[0],
                      'open': float(e[1]),
                      'high': float(e[2]),
                      'low': float(e[3]),
                      'close': float(e[4]),
                      'volume': float(e[5]),
                      'volume_base': float(e[7]),
                      'n_trades': e[8],
                      'volume_bought': float(e[9])} for e in ohlcvs_]
        for e in formatted:
            e['volume_sold'] = e['volume'] - e['volume_bought']
        return formatted

    def iterate_ohlcvs(tss_covered: set):
        ohlcvs = format_ohlcvs(request_klines())
        while True:
            yield ohlcvs
            from_ts = ohlcvs[0]['timestamp']
            while from_ts in tss_covered:
                # sys.stdout.write('\rskipping trades {}  '.format(from_id_))
                from_ts -= timeframe_millis
            from_ts -= (len(ohlcvs) - 1) * timeframe_millis
            ohlcvs = format_ohlcvs(request_klines(from_ts))

    def format_csv_loaded_ohlcvs(csv: pd.DataFrame) -> pd.DataFrame:
        if csv is None:
            return None
        return sort_and_drop_duplicates_by_index(csv[columns].set_index('timestamp'))

    symbol_no_dash = symbol.replace('/', '_')
    timeframe_to_millis_map = {'1m': 60 * 1000,
                               '3m': 3 * 60 * 1000,
                               '5m': 5 * 60 * 1000,
                               '15m': 15 * 60 * 1000,
                               '30m': 30 * 60 * 1000,
                               '1h': 60 * 60 * 1000,
                               '2h': 2 * 60 * 60 * 1000,
                               '4h': 4 * 60 * 60 * 1000,
                               '6h': 6 * 60 * 60 * 1000,
                               '12h': 12 * 60 * 60 * 1000,
                               '1d': 24 * 60 * 60 * 1000,
                               '1w': 7 * 24 * 60 * 60 * 1000,
                               '1M': 30 * 24 * 60 * 60 * 1000
                               }
    timeframe_millis = timeframe_to_millis_map[timeframe]
    dirpath = make_get_filepath('historical_data/ohlcvs_{}/{}/'.format(timeframe, symbol_no_dash))
    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume',
               'volume_base', 'n_trades', 'volume_bought', 'volume_sold']
    cache_path = 'historical_data/ohlcvs_cache/{}_{}.csv'.format(timeframe, symbol_no_dash)
    since = ts_to_day(time() - 60 * 60 * 24 * n_days - 24) if n_days > 0 else '0'
    if not no_download:
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_ohlcvs)
        begin_csv(cache_path, columns)
        ohlcvs_loaded = [format_csv_loaded_ohlcvs(pd.read_csv(dirpath + f))
                         for f in get_filenames(dirpath) if f > since]
        if ohlcvs_loaded:
            ohlcvs_df = sort_and_drop_duplicates_by_index(pd.concat(ohlcvs_loaded))
            tss_covered = set(ohlcvs_df.index)
        else:
            tss_covered = set()
        until_ts = (time() - 60 * 60 * 24 * n_days) * 1000
        for ohlcvs in iterate_ohlcvs(tss_covered):
            write_to_cache(ohlcvs, columns, cache_path)
            print('fetched {} ohlcvs for {} {}'.format(
                timeframe, symbol, ts_to_date(ohlcvs[0]['timestamp'] / 1000)))
            if ohlcvs[0]['timestamp'] <= until_ts:
                break
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_ohlcvs)
    ohlcvs_loaded = [format_csv_loaded_ohlcvs(pd.read_csv(dirpath + f))
                     for f in get_filenames(dirpath) if f > since]
    if len(ohlcvs_loaded) > 0:
        return sort_and_drop_duplicates_by_index(pd.concat(ohlcvs_loaded))


def fetch_raw_trades(symbol: str, n_days: float = 7, no_download: bool = False) -> pd.DataFrame:

    def request_historical_trades(from_id: int = 0) -> dict:
        url = 'https://api.binance.com/api/v3/aggTrades?symbol={}&limit=1000'.format(
            symbol.replace('/', ''))
        if from_id > 0:
            url += '&fromId=' + str(from_id)
        return json.loads(requests.get(url).text)

    def format_raw_trades(trades_: [dict]) -> [dict]:
        return [{'agg_trade_id': t['a'],
                 'price': float(t['p']),
                 'amount': float(t['q']),
                 'timestamp': t['T'],
                 'is_buyer_maker': t['m']} for t in trades_]

    def iterate_raw_trades(ids_covered: set):
        trades = format_raw_trades(request_historical_trades())
        while True:
            yield trades
            from_id_ = trades[0]['agg_trade_id']
            while from_id_ in ids_covered:
                # sys.stdout.write('\rskipping trades {}  '.format(from_id_))
                from_id_ -= 1
            from_id_ -= (len(trades) - 1)
            from_id_ = max(0, from_id_)
            trades = format_raw_trades(request_historical_trades(from_id_))

    def format_csv_loaded_raw_trades(csv: pd.DataFrame) -> pd.DataFrame:
        if csv is None:
            return None
        return sort_and_drop_duplicates_by_index(csv[columns].set_index('agg_trade_id'))

    symbol_no_dash = symbol.replace('/', '_')
    dirpath = make_get_filepath('historical_data/raw_trades/{}/'.format(symbol_no_dash))
    columns = sorted(['agg_trade_id', 'price', 'amount', 'timestamp', 'is_buyer_maker'])
    cache_path = 'historical_data/raw_trades_cache/{}.csv'.format(symbol_no_dash)
    since = ts_to_day(time() - 60 * 60 * 24 * n_days - 24) if n_days > 0 else '0'
    if not no_download:
        clear_cache(cache_path, dirpath, columns, False, format_csv_loaded_raw_trades)
        begin_csv(cache_path, columns)
        raw_trades_loaded = [format_csv_loaded_raw_trades(pd.read_csv(dirpath + f))
                             for f in get_filenames(dirpath) if f > since]
        if raw_trades_loaded:
            raw_trades_df = sort_and_drop_duplicates_by_index(pd.concat(raw_trades_loaded))
            ids_covered = set(raw_trades_df.index)
        else:
            ids_covered = set()
        until_ts = (time() - 60 * 60 * 24 * n_days) * 1000
        rt_tss0 = set()
        for raw_trades in iterate_raw_trades(ids_covered):
            write_to_cache(raw_trades, columns, cache_path)
            print('fetched raw trades for {} {}'.format(
                symbol, ts_to_date(raw_trades[0]['timestamp'] / 1000)))
            if raw_trades[0]['timestamp'] <= until_ts or raw_trades[0]['timestamp'] in rt_tss0:
                break
            rt_tss0.add(raw_trades[0]['timestamp'])
        clear_cache(cache_path, dirpath, columns, False, format_csv_loaded_raw_trades)
    raw_trades_loaded = [format_csv_loaded_raw_trades(pd.read_csv(dirpath + f))
                         for f in get_filenames(dirpath) if f > since]
    if len(raw_trades_loaded) > 0:
        return sort_and_drop_duplicates_by_index(pd.concat(raw_trades_loaded))


def fetch_my_trades_margin(symbol: str,
                           n_days: float = 30,
                           no_download: bool = False,
                           limit: int = 500) -> pd.DataFrame:

    def request_my_trades(from_id: int = -1) -> dict:
        timestamp = int(time() * 1000)
        url = 'https://api.binance.com/sapi/v1/margin/myTrades?'
        params = {'symbol': symbol.replace('/', ''),
                  'limit': limit,
                  'timestamp': timestamp}
        if from_id > 0:
            params['fromId'] = from_id
        query_string = urlencode(params)
        params['signature'] = hmac.new(secret.encode('utf-8'),
                                       query_string.encode('utf-8'),
                                       hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': key}
        return json.loads(requests.get(url, headers=headers, params=params).text)

    def format_my_trades(my_trades_: [dict]) -> [dict]:
        formatted = []
        for t in my_trades_:
            price = float(t['price'])
            amount = float(t['qty'])
            formatted.append(
                {'symbol': symbol,
                 'id': t['id'],
                 'order_id': t['orderId'],
                 'price': price,
                 'amount': amount,
                 'cost': amount * price,
                 'side': 'buy' if t['isBuyer'] else 'sell',
                 'timestamp': t['time'],
                 'datetime': ts_to_date(t['time'] / 1000),
                 'is_maker': t['isMaker'],
                 'fee_cost': float(t['commission']),
                 'fee_currency': t['commissionAsset']}
            )
        return formatted

    def iterate_my_trades(ids_covered: set) -> Iterator[dict]:
        my_trades_ = format_my_trades(request_my_trades())
        while True:
            yield my_trades_
            from_id_ = my_trades_[0]['id']
            while from_id_ in ids_covered:
                from_id_ -= 1
            from_id_ -= len(my_trades_)
            print('from_id_', from_id_)
            new_my_trades_ = format_my_trades(request_my_trades(from_id_))
            my_trades_ = new_my_trades_

    def iterate_my_trades_forwards(ids_covered: set) -> Iterator[dict]:
        my_trades_ = format_my_trades(request_my_trades(0))
        while True:
            yield my_trades_
            from_id_ = my_trades_[-1]['id']
            while from_id_ in ids_covered:
                from_id_ += 1
            new_my_trades_ = format_my_trades(request_my_trades(from_id_))
            my_trades_ = new_my_trades_

    def format_csv_loaded_my_trades(csv: pd.DataFrame) -> pd.DataFrame:
        if csv is None:
            return None
        return sort_and_drop_duplicates_by_index(csv[columns].set_index('id'))

    symbol_no_dash = symbol.replace('/', '_')
    key, secret = load_key_secret('binance')
    dirpath = make_get_filepath('historical_data/my_trades_margin/{}/'.format(symbol_no_dash))
    columns = ['symbol', 'id', 'order_id', 'price', 'amount', 'cost', 'side', 'timestamp',
               'datetime', 'is_maker', 'fee_cost', 'fee_currency']
    cache_path = 'historical_data/my_trades_margin_cache/{}.csv'.format(symbol_no_dash)
    until_ts = (time() - 60 * 60 * 24 * n_days) * 1000
    since = ts_to_day(time() - 60 * 60 * 24 * n_days) if n_days > 0 else '0'
    my_trades_loaded = [format_csv_loaded_my_trades(pd.read_csv(dirpath + f))
                        for f in get_filenames(dirpath) if f > since]
    if not no_download:
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_my_trades)
        begin_csv(cache_path, columns)
        if my_trades_loaded:
            ids_covered = set(pd.concat(my_trades_loaded).index)
        else:
            ids_covered = set()
        for my_trades in iterate_my_trades_forwards(ids_covered):
            if not my_trades or my_trades[-1]['id'] in ids_covered:
                break
            ids_covered.update(list([e['id'] for e in my_trades]))
            write_to_cache(my_trades, columns, cache_path)
            print('fetched my_trades for {} {}'.format(
                symbol, ts_to_date(my_trades[0]['timestamp'] / 1000)))
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_my_trades)
        my_trades_loaded = [format_csv_loaded_my_trades(pd.read_csv(dirpath + f))
                            for f in get_filenames(dirpath) if f > since]
    if len(my_trades_loaded) > 0:
        df = sort_and_drop_duplicates_by_index(pd.concat(my_trades_loaded))
        return df[df['timestamp'] >= until_ts]


def fetch_my_trades(symbol: str,
                    n_days: float = 30,
                    no_download: bool = False,
                    limit: int = 1000) -> pd.DataFrame:

    def request_my_trades(from_id: int = -1) -> dict:
        timestamp = int(time() * 1000)
        url = 'https://api.binance.com/api/v3/myTrades?'
        params = {'symbol': symbol.replace('/', ''),
                  'limit': limit,
                  'timestamp': timestamp}
        if from_id > 0:
            params['fromId'] = from_id
        query_string = urlencode(params)
        params['signature'] = hmac.new(secret.encode('utf-8'),
                                       query_string.encode('utf-8'),
                                       hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': key}
        return json.loads(requests.get(url, headers=headers, params=params).text)

    def format_my_trades(my_trades_: [dict]) -> [dict]:
        formatted = []
        for t in my_trades_:
            price = float(t['price'])
            amount = float(t['qty'])
            formatted.append(
                {'symbol': symbol,
                 'id': t['id'],
                 'order_id': t['orderId'],
                 'price': price,
                 'amount': amount,
                 'cost': amount * price,
                 'side': 'buy' if t['isBuyer'] else 'sell',
                 'timestamp': t['time'],
                 'datetime': ts_to_date(t['time'] / 1000),
                 'is_maker': t['isMaker'],
                 'fee_cost': float(t['commission']),
                 'fee_currency': t['commissionAsset']}
            )
        return formatted

    def iterate_my_trades(ids_covered: set) -> Iterator[dict]:
        my_trades_ = format_my_trades(request_my_trades())
        while True:
            yield my_trades_
            from_id_ = my_trades_[0]['id']
            while from_id_ in ids_covered:
                from_id_ -= 1
            from_id_ -= limit
            new_my_trades_ = format_my_trades(request_my_trades(from_id_))
            my_trades_ = new_my_trades_

    def format_csv_loaded_my_trades(csv: pd.DataFrame) -> pd.DataFrame:
        if csv is None:
            return None
        return sort_and_drop_duplicates_by_index(csv[columns].set_index('id'))

    symbol_no_dash = symbol.replace('/', '_')
    key, secret = load_key_secret('binance')
    dirpath = make_get_filepath('historical_data/my_trades_margin/{}/'.format(symbol_no_dash))
    columns = ['symbol', 'id', 'order_id', 'price', 'amount', 'cost', 'side', 'timestamp',
               'datetime', 'is_maker', 'fee_cost', 'fee_currency']
    cache_path = 'historical_data/my_trades_margin_cache/{}.csv'.format(symbol_no_dash)
    since = ts_to_day(time() - 60 * 60 * 24 * n_days - 24) if n_days > 0 else '0'
    my_trades_loaded = [format_csv_loaded_my_trades(pd.read_csv(dirpath + f))
                        for f in get_filenames(dirpath) if f > since]
    if not no_download:
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_my_trades)
        begin_csv(cache_path, columns)
        if my_trades_loaded:
            ids_covered = set(pd.concat(my_trades_loaded).index)
        else:
            ids_covered = set()
        until_ts = (time() - 60 * 60 * 24 * n_days) * 1000
        prev_id = 0
        for my_trades in iterate_my_trades(ids_covered):
            if my_trades[0]['id'] == prev_id:
                break
            prev_id = my_trades[0]['id']
            write_to_cache(my_trades, columns, cache_path)
            print('fetched my_trades for {} {}'.format(
                symbol, ts_to_date(my_trades[0]['timestamp'] / 1000)))
            if my_trades[0]['timestamp'] <= until_ts:
                break
        clear_cache(cache_path, dirpath, columns, True, format_csv_loaded_my_trades)
        my_trades_loaded = [format_csv_loaded_my_trades(pd.read_csv(dirpath + f))
                            for f in get_filenames(dirpath) if f > since]
    if len(my_trades_loaded) > 0:
        return sort_and_drop_duplicates_by_index(pd.concat(my_trades_loaded))


def main():
    coins = ['ETH', 'XMR', 'NANO', 'EOS', 'ADA', 'IOTA']
    base = 'BTC'
    coins = ['BTC']
    base = 'PAX'
    symbols = sorted([coin + '/' + base for coin in coins])
    print(symbols)
    n_days = 90
    for symbol in symbols:
        print(symbol)
        fetch_raw_trades(symbol, n_days)

if __name__ == '__main__':
    main()









