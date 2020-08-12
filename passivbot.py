from __future__ import annotations
import asyncio
import aiohttp
import json
import websockets
import os
import sys
import random
from common_procedures import print_, load_key_secret, make_get_filepath
from common_functions import ts_to_date, calc_new_ema, remove_duplicates, partition_sorted, \
    round_up, round_dn, flatten
from time import time, sleep
from typing import Callable
from collections import defaultdict
from math import log10

import hmac
import hashlib

from urllib.parse import urljoin, urlencode

HOUR_TO_MILLIS = 60 * 60 * 1000
FORCE_UPDATE_INTERVAL_SECONDS = 60 * 5
MAX_ORDERS_PER_24H = 200000
MAX_ORDERS_PER_10M = MAX_ORDERS_PER_24H / 24 / 6
MAX_ORDERS_PER_10S = 100


async def fetch(url, headers=None, params=None):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params) as response:
            return await response.text()


async def post(url, headers=None, params=None):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(url, params=params) as response:
            return await response.text()


async def delete(url, headers=None, params=None):
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.delete(url, params=params) as response:
            return await response.text()

class ABinance:

    """
    handles fetching of data from binance api
    """

    def __init__(self, user: str):
        self.rest_base_url = 'https://api.binance.com/api/v3/'
        self.margin_base_url = 'https://api.binance.com/sapi/v1/margin/'
        self.key, self.secret = load_key_secret('binance', user)

    async def base_get(self, key: str, kwargs: dict = {}):
        timestamp = int(time() * 1000)
        url = f"{self.margin_base_url}{key}"
        params = {**{'timestamp': timestamp}, **kwargs}
        query_string = urlencode(params)
        params['signature'] = hmac.new(self.secret.encode('utf-8'),
                                       query_string.encode('utf-8'),
                                       hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': self.key}
        return json.loads(await fetch(url, headers, params))

    async def base_post(self, key: str, kwargs: dict = {}):
        timestamp = int(time() * 1000)
        url = f"{self.margin_base_url}{key}"
        params = {**{'timestamp': timestamp}, **kwargs}
        query_string = urlencode(params)
        params['signature'] = hmac.new(self.secret.encode('utf-8'),
                                       query_string.encode('utf-8'),
                                       hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': self.key}
        return json.loads(await post(url, headers, params))

    async def base_delete(self, key: str, kwargs: dict = {}):
        timestamp = int(time() * 1000)
        url = f"{self.margin_base_url}{key}"
        params = {**{'timestamp': timestamp}, **kwargs}
        query_string = urlencode(params)
        params['signature'] = hmac.new(self.secret.encode('utf-8'),
                                       query_string.encode('utf-8'),
                                       hashlib.sha256).hexdigest()
        headers = {'X-MBX-APIKEY': self.key}
        return json.loads(await delete(url, headers, params))

    async def fetch_exchange_info(self):
        return json.loads(await fetch(f"{self.rest_base_url}exchangeInfo"))

    async def fetch_margin_balance(self):
        return await self.base_get('account')

    async def fetch_margin_open_orders(self, symbol: str = None):
        return await self.base_get('openOrders',
                                   {} if symbol is None else {'symbol': symbol.replace('/', '')})

    async def fetch_server_time(self):
        return json.loads(await fetch(f'{self.rest_base_url}time'))

    async def fetch_margin_my_trades(self,
                                     symbol: str,
                                     start_time: int = None,
                                     end_time: int = None,
                                     from_id: int = None,
                                     limit: int = None):
        kwargs = {'symbol': symbol.replace('/', '')}
        for key, kwarg in zip(['startTime', 'endTime', 'fromId', 'limit'],
                              [start_time, end_time, from_id, limit]):
            if kwarg is not None:
                kwargs[key] = kwarg
        return await self.base_get('myTrades', kwargs)

    async def fetch_tickers(self, symbol: str = None):
        url = f"{self.rest_base_url}ticker/bookTicker"
        if symbol is not None:
            kwargs = {'symbol': symbol.replace('/', '')}
            url += '?' + urlencode(kwargs)
        return json.loads(await fetch(url))

    async def fetch_agg_trades(self,
                               symbol: str,
                               from_id: int = None,
                               start_time: int = None,
                               end_time: int = None,
                               limit: int = None):
        kwargs = {'symbol': symbol.replace('/', '')}
        for key, kwarg in zip(['fromId', 'startTime', 'endTime', 'limit'],
                              [from_id, start_time, end_time, limit]):
            if kwarg is not None:
                kwargs[key] = kwarg
        url = f"{self.rest_base_url}aggTrades?" + urlencode(kwargs)
        return json.loads(await fetch(url))

    async def fetch_klines(self,
                           symbol: str,
                           interval: str = '1m',
                           start_time: int = None,
                           end_time: int = None,
                           limit: int = None) -> [dict]:
        """
        allowed_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',
                             '6h', '8h', '12h', '1d', '3d', '1w', '1M'}
        """
        kwargs = {'symbol': symbol.replace('/', ''), 'interval': interval}
        for key, kwarg in zip(['start_time', 'end_time', 'limit'],
                              [start_time, end_time, limit]):
            if kwarg is not None:
                kwargs[key] = kwarg
        url = f"{self.rest_base_url}klines?" + urlencode(kwargs)
        return json.loads(await fetch(url))

    async def fetch_max_borrowable(self, coin: str):
        return await self.base_get('maxBorrowable', {'asset': coin})

    async def borrow(self, coin: str, amount: float):
        return await self.base_post('loan', {'asset': coin, 'amount': str(amount)})

    async def repay(self, coin: str, amount: float):
        return await self.base_post('repay', {'asset': coin, 'amount': str(amount)})

    async def create_margin_bid(self, symbol: str, amount: float, price: float):
        return await self.base_post('order', {'symbol': symbol.replace('/', ''),
                                              'side': 'BUY',
                                              'type': 'LIMIT',
                                              'quantity': str(amount),
                                              'price': str(price),
                                              'timeInForce': 'GTC'})

    async def create_margin_ask(self, symbol: str, amount: float, price: float):
        return await self.base_post('order', {'symbol': symbol.replace('/', ''),
                                              'side': 'SELL',
                                              'type': 'LIMIT',
                                              'quantity': str(amount),
                                              'price': str(price),
                                              'timeInForce': 'GTC'})

    async def cancel_margin_order(self, symbol: str, order_id: int):
        return await self.base_delete('order', {'symbol': symbol.replace('/', ''),
                                                'orderId': str(order_id)})


async def create_bot(settings: dict):
    '''
    if 'BNB' not in settings['coins']:
        settings['coins']['BNB'] = {
            'long': False,
            'shrt': False,
            'borrow': False,
            'account_equity_pct_per_hour': 0.003,
            'account_equity_pct_per_trade': 0.003,
            'entry_spread': 0.001,
            'profit_pct': 0.005,
        }
    '''
    bot = Bot(settings)
    await bot._init()
    return bot

class Bot:
    def __init__(self, settings: dict):
        self.settings = settings
        self.settings['max_memory_span_millis'] = \
            settings['max_memory_span_days'] * 24 * 60 * 60 * 1000
        self.abinance = ABinance(settings['user'])
        self.margin_balance = {}
        self.margin_open_orders = defaultdict(list)
        self.margin_my_trades = defaultdict(dict)
        self.margin_my_trades_analyses = {}
        self.spot_balance = {}
        self.spot_open_orders = defaultdict(dict)
        self.spot_my_trades = defaultdict(dict)

        self.quot = settings['quot']
        self.all_coins = set(list(settings['coins']) + [self.quot])
        self.symbols = set([f'{c}/{self.quot}' for c in sorted(settings['coins'])])

        self.rolling_10s_orders = []
        self.rolling_10m_orders = []

        self.listener = None

        self.minutes_to_seconds = {span: span * 60 for span in settings['ema_spans_minutes']}
        self.emas = defaultdict(dict)
        self.min_ema = {}
        self.max_ema = {}
        self.ema_second = {}
        self.last_price = {}

        self.limits = defaultdict(dict)
        self.ideal_orders = defaultdict(dict)

        self.depth_levels = 5
        self.symbol_formatting_map = {
            s.replace('/', '').lower() + f'@depth{self.depth_levels}': s
            for s in self.symbols
        }
        self.order_book = {}
        self.prev_order_book = self.order_book.copy()
        self.stream_tick_ts = 0
        self.conversion_costs = defaultdict(dict)

    async def _init(self):
        self.balance_log_filepath = \
            make_get_filepath(f"logs/binance/{self.settings['user']}/balance_margin.txt")
        make_get_filepath(f"cache/binance/{self.settings['user']}/account_state/")
        self.exchange_info = await self.abinance.fetch_exchange_info()
        hours_ahead_of_local = round((self.exchange_info['serverTime'] / 1000 - time()) / 3600)
        print_([f'binance is {hours_ahead_of_local} hours ahead of local time'])
        self.now_millis = lambda: (time() + hours_ahead_of_local * 3600) * 1000
        self.nodash_to_dash = {e['symbol']: f"{e['baseAsset']}/{e['quoteAsset']}"
                               for e in self.exchange_info['symbols']}
        self.dash_to_nodash = {v: k for k, v in self.nodash_to_dash.items()}
        self.symbol_split = {s: s.split('/') for s in self.dash_to_nodash}
        self.s2c = {s: self.symbol_split[s][0] for s in self.symbol_split}
        self.longs = {s for s in self.symbols if self.settings['coins'][self.s2c[s]]['long']}
        self.shrts = {s for s in self.symbols if self.settings['coins'][self.s2c[s]]['shrt']}
        self.liquis = {s for s in self.symbols if s not in self.longs and s not in self.shrts}
        self.borrows = {c for c in self.settings['coins'] if self.settings['coins'][c]['borrow']}
        if self.settings['borrow_quot']:
            self.borrows.add(self.quot)
        self.margin_my_asks = {s: [] for s in sorted(self.symbols)}
        self.margin_my_bids = {s: [] for s in sorted(self.symbols)}

        now = time()
        self.timestamps = {'locked': {'execute_to_exchange': now, 'update_margin_balance': now},
                           'released': {'execute_to_exchange': now + 1,
                                        'update_margin_balance': now + 1},
                           'dump_balance_log': 0}
        for key0 in ['update_borrowable', 'borrow', 'repay']:
            self.timestamps['locked'][key0], self.timestamps['released'][key0] = {}, {}
            for key1 in sorted(self.all_coins):
                self.timestamps['locked'][key0][key1] = now
                self.timestamps['released'][key0][key1] = now + 1
        for key0 in ['update_margin_open_orders', 'update_margin_my_trades', 'cancel_margin_order',
                     'create_margin_bid', 'create_margin_ask']:
            self.timestamps['locked'][key0], self.timestamps['released'][key0] = {}, {}
            for key1 in sorted(self.symbols):
                self.timestamps['locked'][key0][key1] = now
                self.timestamps['released'][key0][key1] = now + 1

        self.min_trade_costs = {
            self.nodash_to_dash[e['symbol']]: float(
                next(x for x in e['filters'] if x['filterType'] == 'MIN_NOTIONAL')['minNotional']
            )
            for e in self.exchange_info['symbols']
        }
        self.price_precisions = {
            self.nodash_to_dash[e['symbol']]: -int(round(log10(float(
                next(x for x in e['filters'] if x['filterType'] == 'PRICE_FILTER')['tickSize']
            ))))
            for e in self.exchange_info['symbols']
        }
        self.amount_precisions = {
            self.nodash_to_dash[e['symbol']]: -int(round(log10(float(
                next(x for x in e['filters'] if x['filterType'] == 'LOT_SIZE')['stepSize']
            ))))
            for e in self.exchange_info['symbols']
        }

        sc = self.settings['coins']
        self.entry_spread_plus = {s: 1 + sc[self.s2c[s]]['entry_spread'] / 2 for s in self.symbols}
        self.entry_spread_minus = {s: 1 - sc[self.s2c[s]]['entry_spread'] / 2 for s in self.symbols}
        self.profit_pct_plus = {s: 1 + sc[self.s2c[s]]['profit_pct'] for s in self.symbols}
        self.profit_pct_minus = {s: 1 - sc[self.s2c[s]]['profit_pct'] for s in self.symbols}
        self.account_equity_pct_per_hour = {s: sc[self.s2c[s]]['account_equity_pct_per_hour']
                                            for s in self.symbols}
        self.account_equity_pct_per_trade = {s: sc[self.s2c[s]]['account_equity_pct_per_trade']
                                             for s in self.symbols}

        tickers = await self.abinance.fetch_tickers()
        for e in tickers:
            sdash = self.nodash_to_dash[e['symbol']]
            coin, quot = self.symbol_split[sdash]
            try:
                self.conversion_costs[quot][coin] = 1 / float(e['askPrice'])
                self.conversion_costs[coin][quot] = float(e['bidPrice'])
            except ZeroDivisionError:
                pass
            if sdash in self.symbols:
                self.order_book[sdash] = {
                    's': sdash,
                    'bids': [{'price': float(e['bidPrice']), 'amount': float(e['bidQty'])}],
                    'asks': [{'price': float(e['askPrice']), 'amount': float(e['askQty'])}]
                }
                self.last_price[sdash] = (self.order_book[sdash]['bids'][-1]['price'] +
                                          self.order_book[sdash]['asks'][0]['price']) / 2
        await self.update_margin_balance()
        await self.update_borrowable(self.quot)
        for i in range(0, len((ss := sorted(self.symbols))), (group_size := 7)):
            sgroup = ss[i:i+group_size]
            if not sgroup:
                break
            await asyncio.gather(*flatten([[self.init_ema(s),
                                            self.update_borrowable(self.symbol_split[s][0])]
                                           for s in sgroup]))
            await asyncio.gather(*[self.update_margin_my_trades(s) for s in sgroup])
            await asyncio.gather(*[self.update_margin_open_orders(s) for s in sgroup])
        print_(['finished init'])

    async def start_websocket(self):
        print(self.symbol_formatting_map)
        uri = "wss://stream.binance.com:9443/stream?streams=" + '/'.join(
            list(self.symbol_formatting_map))
        print(uri)
        print('longing', sorted(self.longs))
        print('shrting', sorted(self.shrts))
        print('liquidating', sorted(self.liquis))
        async with websockets.connect(uri) as ws:
            async for msg in ws:
                if msg is None:
                    continue
                ob = json.loads(msg)
                if 'data' not in ob:
                    continue
                symbol = self.symbol_formatting_map[ob['stream']]
                ob = {'s': symbol,
                      'bids': sorted([{'price': float(e[0]), 'amount': float(e[1])}
                                      for e in ob['data']['bids']], key=lambda x: x['price']),
                      'asks': sorted([{'price': float(e[0]), 'amount': float(e[1])}
                                      for e in ob['data']['asks']], key=lambda x: x['price']),
                      'lastUpdateId': ob['data']['lastUpdateId']}
                coin, quot = self.symbol_split[symbol]
                now_second = int(time())
                bid_ask_avg = (ob['bids'][-1]['price'] + ob['asks'][0]['price']) / 2
                if now_second > self.ema_second[symbol]:
                    for span in self.settings['ema_spans_minutes']:
                        self.emas[symbol][span] = calc_new_ema(
                            self.last_price[symbol],
                            bid_ask_avg,
                            self.emas[symbol][span],
                            span=self.minutes_to_seconds[span],
                            n_steps=now_second - self.ema_second[symbol]
                        )
                    self.min_ema[symbol] = min(self.emas[symbol].values())
                    self.max_ema[symbol] = max(self.emas[symbol].values())
                    self.ema_second[symbol] = now_second
                self.prev_order_book[symbol] = self.order_book[symbol]
                self.order_book[symbol] = ob
                self.last_price[symbol] = bid_ask_avg
                self.conversion_costs[coin][quot] = ob['bids'][-1]['price']
                self.conversion_costs[quot][coin] = 1.0 / ob['asks'][0]['price']
                self.stream_tick_ts = time()
                self.on_update(symbol)

    def stop(self):
        self.listener.cancel()
        print('bot stopped')

    async def start(self, do_wait: bool = True):
        if do_wait:
            self.listener = await self.start_websocket()
        else:
            self.listener = asyncio.create_task(self.start_websocket())

    async def init_ema(self, symbol: str):
        print_(['initiating ema', symbol])
        closes = [float(e[4])
                  for e in await self.abinance.fetch_klines(symbol, '1m', limit=1000)]
        for span in self.settings['ema_spans_minutes']:
            ema = closes[0]
            alpha = 2 / (span + 1)
            alpha_ = 1 - alpha
            for close in closes:
                ema = ema * alpha_ + close * alpha
            self.emas[symbol][span] = ema
        self.min_ema[symbol] = min(self.emas[symbol].values())
        self.max_ema[symbol] = max(self.emas[symbol].values())
        self.ema_second[symbol] = int(time())

    def convert_amount(self, amount: float, coin_from: str, coin_to: str):
        try:
            return amount * self.conversion_costs[coin_from][coin_to]
        except KeyError:
            if coin_from == coin_to:
                return amount
            return (amount * self.conversion_costs[coin_from]['BTC'] *
                    self.conversion_costs['BTC'][coin_to])

    def can_execute(self, key0: str, key1: str = None, timeout: int = 12, force: bool = False):
        now = time()
        if key1 is None:
            if force or self.timestamps['released'][key0] > self.timestamps['locked'][key0] or \
                    now - self.timestamps['locked'][key0] > timeout:
                self.timestamps['locked'][key0] = now
                return True
            return False
        else:
            if force or (self.timestamps['released'][key0][key1] >
                         self.timestamps['locked'][key0][key1]) or \
                    now - self.timestamps['locked'][key0][key1] > timeout:
                self.timestamps['locked'][key0][key1] = now
                return True
            return False

    def finished_executing(self, key0: str, key1: str = None):
        if key1 is None:
            self.timestamps['released'][key0] = time()
        else:
            self.timestamps['released'][key0][key1] = time()

    def consume_quota(self):
        now = time()
        self.rolling_10s_orders.append(now)
        self.rolling_10m_orders.append(now)

    async def update_margin_balance(self):
        if not self.can_execute('update_margin_balance'):
            return
        print_(['margin updating balance'])
        fetched = await self.try_wrapper(self.abinance.fetch_margin_balance,
                                         key0='update_margin_balance')
        if fetched is None:
            return

        account_equity = float(fetched['totalNetAssetOfBtc'])
        account_debt = float(fetched['totalLiabilityOfBtc'])
        account_onhand = float(fetched['totalAssetOfBtc'])
        new_balance = {}
        for e in fetched['userAssets']:
            c = e['asset']
            new_balance[c] = {
                'free': float(e['free']),
                'used': float(e['locked']),
                'borrowed': float(e['borrowed']),
                'interest': float(e['interest']),
                'equity': float(e['netAsset']),
                'borrowable': 0.0,
            }
            new_balance[c]['onhand'] = new_balance[c]['free'] + new_balance[c]['used']
            new_balance[c]['debt'] = new_balance[c]['interest'] + new_balance[c]['borrowed']
        if not self.margin_balance:
            self.margin_balance = new_balance
        for c in [self.quot] + list(self.settings['coins']):
            if c not in new_balance:
                new_balance[c] = {key: 0.0 for key in ['free', 'used', 'borrowed', 'interest',
                                                       'onhand', 'equity', 'debt']}
            if c == 'BNB':
                new_balance[c]['onhand'] = \
                    max(0.0, new_balance[c]['onhand'] - self.settings['bnb_buffer'])
            new_balance[c]['account_onhand'] = self.convert_amount(account_onhand, self.quot, c)
            new_balance[c]['account_debt'] = self.convert_amount(account_debt, self.quot, c)
            new_balance[c]['account_equity'] = self.convert_amount(account_equity, self.quot, c)
            new_balance[c]['borrowable'] = self.margin_balance[c]['borrowable']
            symbol = f'{c}/{self.quot}'
            if symbol in self.symbols:
                self.limits[symbol]['entry_cost'] = max([
                    10**-self.amount_precisions[symbol] * self.last_price[symbol],
                    self.min_trade_costs[symbol],
                    (new_balance[self.quot]['account_equity'] *
                     self.account_equity_pct_per_trade[symbol])
                ])
                self.limits[symbol]['min_exit_cost'] = (self.limits[symbol]['entry_cost'] *
                                                        self.settings['min_exit_cost_multiplier'])
                if self.margin_balance[c]['equity'] != new_balance[c]['equity']:
                    asyncio.create_task(self.update_margin_open_orders(symbol))
                    asyncio.create_task(self.update_margin_my_trades(symbol))
                    asyncio.create_task(self.update_borrowable(c))
        self.margin_balance = new_balance
        self.dump_item(new_balance, 'margin_balance')
        if (now := time()) - self.timestamps['dump_balance_log'] > 60 * 60:
            self.dump_balance_log()
            self.timestamps['dump_balance_log'] = now
        self.finished_executing('update_margin_balance')

    def dump_item(self, item: dict, key0: str, key1: str = None):
        fpath = f"cache/binance/{self.settings['user']}/account_state/{key0}"
        if key1 is not None:
            fpath += f"_{key1.replace('/', '_')}"
        json.dump(item, open(f'{fpath}.json', 'w'))

    async def update_margin_open_orders(self, symbol: str):
        if not self.can_execute('update_margin_open_orders', symbol):
            return
        print_(['updating margin open orders', symbol])

        fetched = await self.try_wrapper(self.abinance.fetch_margin_open_orders,
                                         args=(symbol,),
                                         key0='update_margin_open_orders',
                                         key1=symbol,
                                         n_tries=10,
                                         timeout=8)

        if fetched is None:
            return

        self.margin_open_orders[symbol] = []
        self.margin_my_asks[symbol] = []
        self.margin_my_bids[symbol] = []
        formatted = [{'symbol': self.nodash_to_dash[o['symbol']],
                      'id': int(o['orderId']),
                      'timestamp': o['time'],
                      'datetime': ts_to_date(o['time'] / 1000),
                      'type': o['type'].lower(),
                      'side': o['side'].lower(),
                      'price': float(o['price']),
                      'amount': float(o['origQty'])} for o in fetched]
        for o in sorted(formatted, key=lambda x: x['price']):
            if o['side'] == 'buy':
                self.margin_my_bids[symbol].append(o)
            else:
                self.margin_my_asks[symbol].append(o)
            self.margin_open_orders[symbol].append(o)
        self.dump_item(formatted, 'margin_open_orders', symbol)
        self.finished_executing('update_margin_open_orders', symbol)

    async def update_borrowable(self, coin: str):
        if not self.can_execute('update_borrowable', coin):
            return
        if coin not in self.borrows:
            self.margin_balance[coin]['borrowable'] = 0.0
            self.finished_executing('update_borrowable', coin)
            return
        print_(['updating borrowable', coin])
        fetched = await self.try_wrapper(self.abinance.fetch_max_borrowable,
                                         (coin,),
                                         key0='update_borrowable',
                                         key1=coin)
        if fetched is None:
            return
        self.margin_balance[coin]['borrowable'] = float(fetched['amount'])
        self.finished_executing('update_borrowable', coin)

    async def fetch_cached_margin_my_trades(self, symbol: str) -> [dict]:
        no_dash = symbol.replace('/', '_')
        cache_filepath = make_get_filepath(
            f"cache/binance/{self.settings['user']}/my_trades/{no_dash}/")
        filenames = [fp for fp in os.listdir(cache_filepath) if fp.endswith('txt')]
        cached_history = []
        for filename in sorted(filenames):
            with open(cache_filepath + filename) as f:
                cached_history += [json.loads(line) for line in f.readlines()]
        cached_history = remove_duplicates(cached_history, key='id', sort=True)
        if not cached_history:
            limit = 1000
            from_id = 0
        else:
            limit = 100
            from_id = cached_history[-1]['id'] + 1
        fetched_history = []
        prev_my_trades = []
        while True:
            my_trades = [{
                'symbol': symbol,
                'id': t['id'],
                'order_id': t['orderId'],
                'side': 'buy' if t['isBuyer'] else 'sell',
                'amount': (amount := float(t['qty'])),
                'price': (price := float(t['price'])),
                'cost': amount * price,
                'timestamp': t['time'],
                'datetime': ts_to_date(t['time'] / 1000),
                'is_maker': t['isMaker'],
            } for t in await self.abinance.fetch_margin_my_trades(symbol,
                                                                  limit=limit,
                                                                  from_id=from_id)]
            fetched_history += my_trades
            if len(my_trades) < limit or my_trades == prev_my_trades:
                break
            print_(['fetched my trades', symbol, my_trades[0]['datetime']])
            prev_my_trades = my_trades
            limit = 1000
            from_id = my_trades[-1]['id'] + 1
            sleep(1)
        written_history = write_cache(cache_filepath, fetched_history)
        my_trades = remove_duplicates(cached_history + written_history,
                                      key='id', sort=True)
        return my_trades

    async def update_margin_my_trades(self, symbol: str):
        if not self.can_execute('update_margin_my_trades', symbol):
            return
        print_(['updating margin my trades', symbol])
        fetched = await self.try_wrapper(self.fetch_cached_margin_my_trades,
                                         (symbol,),
                                         key0='update_margin_my_trades',
                                         key1=symbol,
                                         timeout=360, n_tries=1)
        if fetched is None:
            return
        age_limit_millis = max(
            (fetched[-1]['timestamp'] if fetched else 0) - self.settings['max_memory_span_millis'],
            self.settings['snapshot_timestamp_millis']
        )
        my_trades_cropped, analysis = analyze_my_trades(
            [e for e in fetched if e['timestamp'] >= age_limit_millis],
            (self.limits[symbol]['entry_cost'] *
             (self.settings['min_exit_cost_multiplier'] * 0.75) /
             self.last_price[symbol])
        )
        analysis['long_sel_price'] = round_up(
            analysis['long_vwap'] * self.profit_pct_plus[symbol],
            self.price_precisions[symbol]
        )
        analysis['shrt_buy_price'] = round_dn(
            analysis['shrt_vwap'] * self.profit_pct_minus[symbol],
            self.price_precisions[symbol]
        )
        self.margin_my_trades_analyses[symbol] = analysis
        self.margin_my_trades[symbol] = my_trades_cropped
        self.finished_executing('update_margin_my_trades', symbol)

    def on_update(self, s: str):
        if self.order_book[s]['bids'][-1]['price'] != self.prev_order_book[s]['bids'][-1]['price']:
            #self.print_order_book()
            if self.order_book[s]['bids'][-1]['price'] < \
                    self.prev_order_book[s]['bids'][-1]['price']:
                if self.margin_my_bids[s] and \
                        self.order_book[s]['bids'][-1]['price'] <= \
                        self.margin_my_bids[s][-1]['price']:
                    print_(['margin bid taken', s])
                    asyncio.create_task(self.update_margin_balance())
        if self.order_book[s]['asks'][0]['price'] != self.prev_order_book[s]['asks'][0]['price']:
            #self.print_order_book()
            if self.order_book[s]['asks'][0]['price'] > \
                    self.prev_order_book[s]['asks'][0]['price']:
                if self.margin_my_asks[s] and \
                        self.order_book[s]['asks'][0]['price'] >= \
                        self.margin_my_asks[s][0]['price']:
                    print_(['margin ask taken', s])
                    asyncio.create_task(self.update_margin_balance())
        asyncio.create_task(self.execute_to_exchange())

    async def force_update(self,
                           key1: str = None,
                           timeout: int = FORCE_UPDATE_INTERVAL_SECONDS,
                           do_wait: bool = False) -> bool:
        now = time()
        tasks = []
        if now - self.timestamps['released']['update_margin_balance'] > timeout:
            tasks.append(self.update_margin_balance())
        for key1_ in (self.timestamps['released']['update_borrowable']
                      if key1 is None else [self.s2c[key1]]):
            if now - self.timestamps['released']['update_borrowable'][key1_] > timeout:
                tasks.append(self.update_borrowable(key1_))
        for key0 in ['update_margin_open_orders', 'update_margin_my_trades']:
            for key1_ in self.timestamps['released'][key0] if key1 is None else [key1]:
                if now - self.timestamps['released'][key0][key1_] > timeout:
                    tasks.append(getattr(self, key0)(key1_))
        if tasks:
            if do_wait:
                for i in range(0, len(tasks), (step := 7)):
                    task_group = tasks[i:i + step]
                    await asyncio.gather(*task_group)
            else:
                for task in tasks:
                    asyncio.create_task(task)
            return True
        return False

    def force_release(self, timeout: int = 12) -> bool:
        now = time()
        all_clear = True
        for key0 in self.timestamps['released']:
            if type(self.timestamps['released'][key0]) == dict:
                for key1 in self.timestamps['released'][key0]:
                    if self.timestamps['released'][key0][key1] < \
                            self.timestamps['locked'][key0][key1]:
                        if now - self.timestamps['locked'][key0][key1] > timeout:
                            print_(['force released', key0, key1])
                            self.timestamps['released'][key0][key1] = now
                        else:
                            all_clear = False
            else:
                if self.timestamps['released'][key0] < self.timestamps['locked'][key0]:
                    if now - self.timestamps['locked'][key0] > timeout:
                        self.timestamps['released'][key0] = now
                        print_(['force released', key0])
                    else:
                        all_clear = False
        return all_clear

    async def execute_to_exchange(self):
        now = time()
        if now - self.timestamps['released']['execute_to_exchange'] < 0.5:
            return
        if await self.force_update():
            return
        if not self.force_release():
            return
        if not self.can_execute('execute_to_exchange'):
            return

        for s in self.symbols:
            self.set_ideal_margin_orders(s)
        entries, exits, borrows, repays = self.allocate_credit()
        order_deletions, order_creations = filter_orders(flatten(self.margin_open_orders.values()),
                                                         entries + exits)
        # cancel and borrow first
        future_results = []
        available_quota = self.get_execution_quota()
        n_orders = min(MAX_ORDERS_PER_10S, available_quota) // 2
        if borrows:
            borrow_ = random.choice(borrows)
            future_results.append(asyncio.create_task(
                self.borrow(borrow_['coin'], borrow_['amount'])
            ))
            await asyncio.sleep(0.1)
        for od in order_deletions[:n_orders]:
            future_results.append(asyncio.create_task(
                self.cancel_margin_order(od['symbol'], od['id'])
            ))
        await asyncio.sleep(0.1)
        if not borrows and repays:
            repay_ = \
                sorted(repays, key=lambda x: self.timestamps['released']['repay'][x['coin']])[0]
            if now - self.timestamps['released']['repay'][repay_['coin']] > 60 * 58:
                future_results.append(asyncio.create_task(
                    self.repay(repay_['coin'], repay_['amount'])
                ))
                await asyncio.sleep(0.1)
        for oc in order_creations[:n_orders]:
            if oc['side'] == 'buy':
                future_results.append(asyncio.create_task(
                    self.create_margin_bid(oc['symbol'], oc['amount'], oc['price'], block=False)
                ))
            else:
                future_results.append(asyncio.create_task(
                    self.create_margin_ask(oc['symbol'], oc['amount'], oc['price'], block=False)
                ))
        await asyncio.gather(*future_results)
        self.finished_executing('execute_to_exchange')
        return borrows, repays, order_deletions, order_creations, future_results

    def get_execution_quota(self):

        def new_list(timestamps: list, ts_limit: float):
            for i, ts in enumerate(timestamps):
                if ts > ts_limit:
                    return timestamps[i:]
            return []

        now = time()
        self.rolling_10m_orders = new_list(self.rolling_10m_orders, now - 600)
        self.rolling_10s_orders = new_list(self.rolling_10s_orders, now - 10)
        return min(MAX_ORDERS_PER_10M - len(self.rolling_10m_orders),
                   MAX_ORDERS_PER_10S - len(self.rolling_10s_orders))

    async def try_wrapper(self,
                          fn: Callable,
                          args: tuple = (),
                          kwargs: dict = {},
                          key0: str = None,
                          key1: str = None,
                          n_tries=4,
                          timeout=5):
        for k in range(n_tries):
            try:
                result = await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout)
                return result
            except asyncio.TimeoutError:
                if key0 is not None:
                    if key1 is None:
                        self.timestamps['locked'][key0] = time()
                    else:
                        self.timestamps['locked'][key0][key1] = time()
                line = f"timeout {fn.__name__} {args if args else ''} {kwargs if kwargs else ''}"
                line += f" attempt {k + 2} of {n_tries}"
                print_([line])
        else:
            raise Exception(f'too many tries, {fn.__name__}')

    def set_ideal_margin_orders(self, s: str):

        coin, quot = self.symbol_split[s]

        # prepare data
        other_bids = calc_other_orders(self.margin_my_bids[s], self.order_book[s]['bids'],
                                       self.price_precisions[s])
        other_asks = calc_other_orders(self.margin_my_asks[s], self.order_book[s]['asks'],
                                       self.price_precisions[s])

        highest_other_bid = sorted(other_bids, key=lambda x: x['price'])[-1] \
            if other_bids else self.margin_my_bids[s][-1]
        lowest_other_ask = sorted(other_asks, key=lambda x: x['price'])[0] \
            if other_asks else self.margin_my_asks[s][0]

        other_bid_incr = round(highest_other_bid['price'] + 10**-self.price_precisions[s],
                               self.price_precisions[s])
        other_ask_decr = round(lowest_other_ask['price'] - 10**-self.price_precisions[s],
                               self.price_precisions[s])

        entry_cost = self.limits[s]['entry_cost']

        # set ideal orders
        exponent = self.settings['entry_vol_modifier_exponent']
        now_millis = self.now_millis()
        if self.settings['coins'][coin]['shrt']:
            shrt_sel_price = max([
                round_up(self.max_ema[s] * self.entry_spread_plus[s],
                         self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr if entry_cost / other_ask_decr < lowest_other_ask['amount']
                 else lowest_other_ask['price'])
            ])
            shrt_amount_modifier = max(
                1.0,
                min(
                    self.settings['min_exit_cost_multiplier'] / 2,
                    (self.last_price[s] /
                     self.margin_my_trades_analyses[s]['shrt_buy_price'])**exponent
                )
            ) if self.margin_my_trades_analyses[s]['shrt_buy_price'] > 0.0 else 1.0
            if now_millis - self.margin_my_trades_analyses[s]['last_shrt_entry_ts'] > \
                    (HOUR_TO_MILLIS *
                     min(self.margin_my_trades_analyses[s]['last_shrt_entry_cost'], entry_cost) /
                     (self.account_equity_pct_per_hour[s] *
                      self.margin_balance[quot]['account_equity'])):
                shrt_sel_amount = entry_cost * shrt_amount_modifier / shrt_sel_price
            else:
                shrt_sel_amount = 0.0
            self.ideal_orders[s]['shrt_sel'] = {
                'symbol': s, 'side': 'sell',
                'amount': (ssar if (ssar := round_up(shrt_sel_amount, self.amount_precisions[s])) *
                           shrt_sel_price >= self.min_trade_costs[s] else 0.0),
                'price': shrt_sel_price
            }
            shrt_buy_amount = round_up(self.margin_my_trades_analyses[s]['shrt_amount'],
                                       self.amount_precisions[s])
            self.ideal_orders[s]['shrt_buy'] = {
                'symbol': s, 'side': 'buy',
                'amount': shrt_buy_amount,
                'price': min([round_dn(self.min_ema[s], self.price_precisions[s]),
                              other_ask_decr,
                              (other_bid_incr if shrt_buy_amount < highest_other_bid['amount']
                               else highest_other_bid['price']),
                              self.margin_my_trades_analyses[s]['shrt_buy_price']])
            }
        if self.settings['coins'][coin]['long']:
            long_buy_price = min([
                round_dn(self.min_ema[s] * self.entry_spread_minus[s],
                         self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr if entry_cost / other_bid_incr < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            long_amount_modifier = max(
                1.0,
                min(
                    self.settings['min_exit_cost_multiplier'] / 2,
                    (self.margin_my_trades_analyses[s]['long_sel_price'] /
                     self.last_price[s])**exponent
                )
            )
            if now_millis - self.margin_my_trades_analyses[s]['last_long_entry_ts'] > \
                    (HOUR_TO_MILLIS *
                     min(self.margin_my_trades_analyses[s]['last_long_entry_cost'], entry_cost) /
                     (self.account_equity_pct_per_hour[s] *
                      self.margin_balance[quot]['account_equity'])):
                long_buy_amount = entry_cost * long_amount_modifier / long_buy_price
            else:
                long_buy_amount = 0.0
            self.ideal_orders[s]['long_buy'] = {
                'symbol': s, 'side': 'buy',
                'amount': (lbar if (lbar := round_up(long_buy_amount, self.amount_precisions[s])) *
                           long_buy_price >= self.min_trade_costs[s] else 0.0),
                'price': long_buy_price
            }
            long_sel_amount = round_up(self.margin_my_trades_analyses[s]['long_amount'],
                                       self.amount_precisions[s])
            self.ideal_orders[s]['long_sel'] = {
                'symbol': s, 'side': 'sell',
                'amount': long_sel_amount,
                'price': max([round_up(self.max_ema[s], self.price_precisions[s]),
                              other_bid_incr,
                              (other_ask_decr if long_sel_amount < lowest_other_ask['amount']
                               else lowest_other_ask['price']),
                              self.margin_my_trades_analyses[s]['long_sel_price']])
            }
        if not any([self.settings['coins'][coin][k] for k in ['long', 'shrt']]):
            liqui_cost = max(self.min_trade_costs[s],
                             self.limits[s]['min_exit_cost'] / 2)
            bid_price = min([
                round_dn(self.min_ema[s], self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr if liqui_cost / other_bid_incr < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            ask_price = max([
                round_up(self.max_ema[s], self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr if liqui_cost / other_ask_decr < lowest_other_ask['amount']
                 else lowest_other_ask['price'])])
            repay_amount = min(self.margin_balance[coin]['debt'],
                               self.margin_balance[coin]['onhand'])
            bid_amount = max(
                0.0,
                round_up(min(liqui_cost / bid_price,
                             self.margin_balance[coin]['debt'] - self.margin_balance[coin]['onhand']),
                         self.amount_precisions[s])
            )
            ask_amount = max(
                0.0,
                min(round_up(liqui_cost / ask_price, self.amount_precisions[s]),
                    round_dn(self.margin_balance[coin]['onhand'] - self.margin_balance[coin]['debt'],
                             self.amount_precisions[s]))
            )
            if ask_amount * ask_price > self.min_trade_costs[s] and ask_price > 0.0:
                self.ideal_orders[s]['liqui_sel'] = {'symbol': s, 'side': 'sell',
                                                     'amount': ask_amount,
                                                     'price': ask_price}
            else:
                self.ideal_orders[s]['liqui_sel'] = {'symbol': s, 'side': 'sell', 'amount': 0.0,
                                                     'price': 0.0}
            if bid_amount * bid_price > self.min_trade_costs[s] and ask_price > 0.0:
                self.ideal_orders[s]['liqui_buy'] = {'symbol': s, 'side': 'buy',
                                                     'amount': bid_amount,
                                                     'price': bid_price}
            else:
                self.ideal_orders[s]['liqui_buy'] = {'symbol': s, 'side': 'buy', 'amount': 0.0,
                                                     'price': 0.0}
            if repay_amount > 0.0:
                self.ideal_orders[s]['repay'] = repay_amount

    def allocate_credit(self) -> ([dict], [dict], [dict]):
        # allocate credit and select eligible orders
        # default repay all debt
        # borrow to cover entries
        # if any exit is close to filling, borrow to cover exit(s)
        credit_available = {c: self.margin_balance[c]['borrowable'] for c in self.all_coins}
        max_credit_avbl_quot = max([self.convert_amount(credit_available[c], c, self.quot)
                                    for c in credit_available])
        borrows = {c: 0.0 for c in self.all_coins}
        coin_available = {c: self.margin_balance[c]['onhand'] for c in self.all_coins}
        long_buys, shrt_sels = [], []
        entries = []
        for s in self.longs:
            if all([self.ideal_orders[s]['long_buy'][key] > 0.0 for key in ['price', 'amount']]):
                entries.append(
                    {**{'lp_diff': self.last_price[s] / self.ideal_orders[s]['long_buy']['price']},
                     **self.ideal_orders[s]['long_buy']}
                )
        for s in self.shrts:
            if all([self.ideal_orders[s]['shrt_sel'][key] > 0.0 for key in ['price', 'amount']]):
                entries.append(
                    {**{'lp_diff': self.ideal_orders[s]['shrt_sel']['price'] / self.last_price[s]},
                     **self.ideal_orders[s]['shrt_sel']}
                )
        eligible_entries = []
        for entry in sorted(entries, key=lambda x: x['lp_diff']):
            s = entry['symbol']
            c, q = self.symbol_split[s]
            if entry['side'] == 'sell':
                if coin_available[c] > entry['amount']:
                    eligible_entries.append(entry)
                    coin_available[c] -= entry['amount']
                else:
                    if credit_available[c] + coin_available[c] >= entry['amount']:
                        eligible_entries.append(entry)
                        borrow_amount = entry['amount'] - coin_available[c]
                        coin_available[c] = 0.0
                        credit_available[c] -= borrow_amount
                        max_credit_avbl_quot -= borrow_amount * entry['price']
                        borrows[c] += borrow_amount
                    else:
                        borrow_amount = credit_available[c]
                        entry['amount'] = round_dn(coin_available[c] + borrow_amount,
                                                   self.amount_precisions[s])
                        if entry['amount'] * entry['price'] >= self.min_trade_costs[s]:
                            eligible_entries.append(entry)
                            coin_available[c] = 0.0
                            max_credit_avbl_quot -= borrow_amount * entry['price']
                            credit_available[c] = 0.0
                            borrows[c] += borrow_amount
            else:
                entry_cost = entry['amount'] * entry['price']
                if coin_available[q] >= entry_cost:
                    eligible_entries.append(entry)
                    coin_available[q] -= entry_cost
                else:
                    if credit_available[q] + coin_available[q] >= entry_cost:
                        eligible_entries.append(entry)
                        borrow_amount = entry_cost - coin_available[q]
                        coin_available[q] = 0.0
                        credit_available[q] -= borrow_amount
                        max_credit_avbl_quot -= borrow_amount
                        borrows[q] += borrow_amount
                    else:
                        borrow_amount = credit_available[q]
                        entry['amount'] = round_dn(
                            (coin_available[q] + borrow_amount) / entry['price'],
                            self.amount_precisions[s]
                        )
                        if entry['amount'] * entry['price'] >= self.min_trade_costs[s]:
                            eligible_entries.append(entry)
                            coin_available[q] = 0.0
                            max_credit_avbl_quot -= borrow_amount
                            credit_available[q] = 0.0
                            borrows[q] += borrow_amount
        exits = []
        for s in self.longs:
            if self.settings['coins'][self.s2c[s]]['long'] and \
                    (self.ideal_orders[s]['long_sel']['amount'] *
                     self.ideal_orders[s]['long_sel']['price'] >=
                     self.limits[s]['min_exit_cost']):
                exits.append(
                    {**{'lp_diff': self.ideal_orders[s]['long_sel']['price'] / self.last_price[s]},
                     **self.ideal_orders[s]['long_sel']}
                )
        for s in self.shrts:
            if self.settings['coins'][self.s2c[s]]['shrt'] and \
                    (self.ideal_orders[s]['shrt_buy']['amount'] *
                     self.ideal_orders[s]['shrt_buy']['price'] >=
                     self.limits[s]['min_exit_cost']):
                try:
                    lp_diff = self.last_price[s] / self.ideal_orders[s]['long_sel']['price']
                except ZeroDivisionError:
                    lpdiff = 1.0
                exits.append(
                    {**{'lp_diff': self.last_price[s] / self.ideal_orders[s]['shrt_buy']['price']},
                     **self.ideal_orders[s]['shrt_buy']}
                )
        eligible_exits = []
        for exit in sorted(filter(lambda x: x['lp_diff'] < 1.1, exits), key=lambda x: x['lp_diff']):
            s = exit['symbol']
            c, q = self.symbol_split[s]
            if exit['side'] == 'sell':
                if exit['amount'] > coin_available[c]:
                    # not enough coin for full long exit
                    borrow_amount = min([credit_available[c],
                                         max_credit_avbl_quot / exit['price'],
                                         exit['amount'] - coin_available[c]])
                    partial_amount = round_dn(round(borrow_amount + coin_available[c],
                                                    self.amount_precisions[s] + 1),
                                              self.amount_precisions[s])
                    if partial_amount * exit['price'] >= self.limits[s]['min_exit_cost']:
                        # partial exit
                        exit['amount'] = partial_amount
                        eligible_exits.append(exit)
                        coin_available[c] += borrow_amount - exit['amount']
                        credit_available[c] -= borrow_amount
                        max_credit_avbl_quot -= borrow_amount * exit['price']
                        borrows[c] += borrow_amount
                else:
                    # full exit no borrow
                    # sell all with leftovers
                    exit['amount'] = round_dn(coin_available[c], self.amount_precisions[s])
                    eligible_exits.append(exit)
                    coin_available[c] -= exit['amount']
            else:
                exit_cost = exit['amount'] * exit['price']
                if exit_cost > coin_available[q]:
                    # not enough quot for full shrt exit
                    borrow_amount = min([credit_available[q],
                                         max_credit_avbl_quot,
                                         exit_cost - coin_available[q]])
                    partial_amount = round_dn(
                        round((borrow_amount + coin_available[q]) / exit['price'],
                              self.amount_precisions[s] + 1),
                        self.amount_precisions[s]
                    )
                    partial_cost = partial_amount * exit['price']
                    if partial_cost >= self.limits[s]['min_exit_cost']:
                        exit['amount'] = partial_amount
                        eligible_exits.append(exit)
                        coin_available[q] += borrow_amount - partial_cost
                        credit_available[q] -= borrow_amount
                        max_credit_avbl_quot -= borrow_amount
                        borrows[q] += borrow_amount
                else:
                    # full exit no borrow
                    # adjust amount to pay off all debt
                    exit['amount'] = min(
                        round_up(max(self.margin_balance[c]['debt'], exit['amount']),
                                 self.amount_precisions[s]),
                        round_dn(coin_available[q] / exit['price'], self.amount_precisions[s])
                    )
                    exit_cost = exit['amount'] * exit['price']
                    eligible_exits.append(exit)
                    coin_available[q] -= exit_cost
        for s in self.liquis:
            c, q = self.symbol_split[s]
            lb = self.ideal_orders[s]['liqui_buy']
            lb_cost = lb['amount'] * lb['price']
            if lb_cost > 0.0:
                if coin_available[q] >= lb_cost:
                    eligible_entries.append(lb)
                    coin_available[q] -= lb_cost
            ls = self.ideal_orders[s]['liqui_sel']
            if ls['amount'] > 0.0 and ls['price'] > 0.0:
                eligible_entries.append(ls)
                coin_available[c] -= ls['amount']

        eligible_borrows, eligible_repays = [], []
        for c in borrows:
            borrows[c] -= min(self.margin_balance[c]['debt'], coin_available[c])
            borrow_amount = round(min(borrows[c], self.margin_balance[c]['borrowable']), 8)
            if c in self.borrows:
                if borrow_amount > 0.0:
                    eligible_borrows.append({'coin': c, 'side': 'borrow', 'amount': borrow_amount})
                elif borrow_amount < 0.0:
                    eligible_repays.append({'coin': c, 'side': 'repay', 'amount': -borrow_amount})
            elif (repay_amount := min(self.margin_balance[c]['onhand'],
                                      self.margin_balance[c]['debt'])) > 0.0:
                eligible_repays.append({'coin': c, 'side': 'repay', 'amount': repay_amount})
        return eligible_entries, eligible_exits, eligible_borrows, eligible_repays

    async def create_margin_bid(self, symbol: str, amount: float, price: float, block: bool = True):
        if not self.can_execute('create_margin_bid', symbol, force=not block):
            return
        self.consume_quota()
        fetched = await self.abinance.create_margin_bid(symbol, amount, price)
        if 'status' in fetched and fetched['status'] == 'NEW':
            bid = {'symbol': symbol, 'side': 'buy', 'amount': float(fetched['origQty']),
                   'price': float(fetched['price']), 'id': fetched['orderId']}
            self.margin_open_orders[symbol].append(bid)
            self.margin_my_bids[symbol] = sorted(self.margin_my_bids[symbol] + [bid],
                                                 key=lambda x: x['price'])
            c, q = self.symbol_split[symbol]
            self.margin_balance[q]['free'] -= bid['price'] * bid['amount']
            line = f"  created margin bid {symbol:10}{bid['amount']:12}{bid['price']:12}"
            line += f"{bid['id']:12}"
            print_([line])
        elif 'code' in fetched:
            print_(['failed to create bid', symbol, amount, price, fetched])
            await asyncio.gather(self.update_margin_balance(),
                                 self.update_margin_open_orders(symbol),
                                 self.update_margin_my_trades(symbol))
        self.finished_executing('create_margin_bid', symbol)
        return fetched

    async def create_margin_ask(self, symbol: str, amount: float, price: float, block: bool = True):
        if not self.can_execute('create_margin_ask', symbol, force=not block):
            return
        self.consume_quota()
        fetched = await self.abinance.create_margin_ask(symbol, amount, price)
        if 'status' in fetched and fetched['status'] == 'NEW':
            ask = {'symbol': symbol, 'side': 'sell', 'amount': float(fetched['origQty']),
                   'price': float(fetched['price']), 'id': fetched['orderId']}
            self.margin_open_orders[symbol].append(ask)
            self.margin_my_asks[symbol] = sorted(self.margin_my_asks[symbol] + [ask],
                                                 key=lambda x: x['price'])
            c, q = self.symbol_split[symbol]
            self.margin_balance[c]['free'] -= ask['amount']
            self.margin_balance[c]['used'] += ask['amount']
            line = f"  created margin ask {symbol:10}{ask['amount']:12}{ask['price']:12}"
            line += f"{ask['id']:12}"
            print_([line])
        elif 'code' in fetched:
            print_(['failed to create ask', symbol, amount, price, fetched])
            await asyncio.gather(self.update_margin_balance(),
                                 self.update_margin_open_orders(symbol),
                                 self.update_margin_my_trades(symbol))
        self.finished_executing('create_margin_ask', symbol)
        return fetched

    async def cancel_margin_order(self, symbol: str, order_id: int):
        self.consume_quota()
        cancelled = await self.abinance.cancel_margin_order(symbol, order_id)
        if 'status' in cancelled and cancelled['status'] == 'CANCELED':
            self.margin_open_orders[symbol] = \
                [o for o in self.margin_open_orders[symbol] if o['id'] != order_id]
            c, q = self.symbol_split[symbol]
            amount = float(cancelled['origQty']) - float(cancelled['executedQty'])
            if cancelled['side'] == 'BUY':
                self.margin_my_bids[symbol] = \
                    [o for o in self.margin_my_bids[symbol] if o['id'] != order_id]
                cost = float(cancelled['price']) * amount
                self.margin_balance[q]['free'] += cost
                self.margin_balance[q]['used'] -= cost
            else:
                self.margin_my_asks[symbol] = \
                    [o for o in self.margin_my_asks[symbol] if o['id'] != order_id]
                self.margin_balance[c]['free'] += amount
                self.margin_balance[c]['used'] -= amount
            line = f"cancelled margin {'bid' if cancelled['side'] == 'BUY' else 'ask'} "
            line += f"{symbol:10}{float(cancelled['origQty']):12}{float(cancelled['price']):12}"
            line += f"{order_id:12}"
            print_([line])
        elif 'code' in cancelled:
            if cancelled['code'] == -2011:
                print_(['trying to cancel non existent order'])
                asyncio.create_task(self.update_margin_balance())
                await asyncio.gather(self.update_margin_balance(),
                                     self.update_margin_open_orders(symbol),
                                     self.update_margin_my_trades(symbol))
            print_(['failed to cancel order', symbol, cancelled])
        return cancelled

    async def borrow(self, coin: str, amount: float):
        if not self.can_execute('borrow', coin):
            return
        borrowed = await self.abinance.borrow(coin, amount)
        if 'tranId' in borrowed:
            self.margin_balance[coin]['borrowable'] -= amount
            self.margin_balance[coin]['free'] += amount
            self.margin_balance[coin]['onhand'] += amount
            self.margin_balance[coin]['debt'] += amount
            print_(['borrowed', amount, coin, borrowed])
        else:
            print_(['failed to borrow', amount, coin, borrowed])
            await asyncio.gather(self.update_margin_balance(), self.update_borrowable(coin))
        self.finished_executing('borrow', coin)
        return borrowed



    async def repay(self, coin: str, amount: float):
        if not self.can_execute('repay', coin):
            return
        repaid = await self.abinance.repay(coin, amount)
        if 'tranId' in repaid:
            asyncio.create_task(self.update_margin_balance())
            print_(['repaid', amount, coin, repaid])
        else:
            print_(['failed to repay', amount, coin, repaid])
            await asyncio.gather(self.update_margin_balance(), self.update_borrowable(coin))
        self.finished_executing('repay', coin)
        return repaid

    def dump_balance_log(self):
        print_(['margin dumping balance'])
        with open(self.balance_log_filepath, 'a') as f:
            line = json.dumps({**{'timestamp': self.now_millis()}, **self.margin_balance}) + '\n'
            f.write(line)

    def print_state(self):
        pass


def calc_other_orders(my_orders: [dict],
                      order_book: [dict],
                      price_precision: int = 8) -> [dict]:
    # my_orders = [{'price': float, 'amount': float}]
    other_orders = {o['price']: o['amount'] for o in order_book}
    for o in my_orders:
        if o['price'] in other_orders:
            if o['amount'] >= other_orders[o['price']]:
                del other_orders[o['price']]
            else:
                other_orders[o['price']] = round(other_orders[o['price']] - o['amount'],
                                                 price_precision)
    return [{'price': p, 'amount': other_orders[p]}
            for p in sorted(other_orders)]


def load_settings(user: str):
    default_settings = json.load(open('settings/binance/default.json'))
    try:
        settings = json.load(open(f'settings/binance/{user}.json'))
        for k0 in default_settings:
            if k0 not in settings:
                settings[k0] = default_settings[k0]
    except FileNotFoundError:
        print(f'{user} not found, using default settings')
        settings = default_settings
    settings['user'] = user
    return settings


def write_cache(filepath: str, items: [dict], condition: Callable = lambda x: True):
    written_items = []
    for d in remove_duplicates(items, key='id', sort=True):
        if condition(d):
            month = ts_to_date(d['timestamp'] / 1000)[:7]
            with open(f'{filepath}{month}.txt', 'a') as f:
                f.write(json.dumps(d) + '\n')
            written_items.append(d)
    return written_items


def analyze_my_trades(my_trades: [dict], entry_exit_amount_threshold: float) -> ([dict], dict):

    long_cost, long_amount = 0.0, 0.0
    shrt_cost, shrt_amount = 0.0, 0.0

    long_start_ts, shrt_start_ts = 0, 0
    last_long_entry_ts, last_shrt_entry_ts = 0, 0
    last_long_entry_cost, last_shrt_entry_cost = 1e-10, 1e-10

    for mt in my_trades:
        if mt['side'] == 'buy':
            if mt['amount'] < entry_exit_amount_threshold:
                # long buy
                long_amount += mt['amount']
                long_cost += mt['cost']
                last_long_entry_ts = mt['timestamp']
                last_long_entry_cost = mt['cost']
            else:
                # shrt buy
                shrt_amount -= mt['amount']
                shrt_cost -= mt['cost']
                if shrt_amount <= 0.0 or shrt_cost <= 0.0:
                    shrt_start_ts = mt['timestamp']
                    shrt_amount = 0.0
                    shrt_cost = 0.0
        else:
            if mt['amount'] < entry_exit_amount_threshold:
                # shrt sel
                shrt_amount += mt['amount']
                shrt_cost += mt['cost']
                last_shrt_entry_ts = mt['timestamp']
                last_shrt_entry_cost = mt['cost']
            else:
                # long sel
                long_amount -= mt['amount']
                long_cost -= mt['cost']
                if long_amount <= 0.0 or long_cost <= 0.0:
                    long_start_ts = mt['timestamp']
                    long_amount = 0.0
                    long_cost = 0.0

    analysis = {'long_amount': long_amount,
                'long_cost': long_cost,
                'long_vwap': long_cost / long_amount if long_amount else 0.0,
                'shrt_amount': shrt_amount,
                'shrt_cost': shrt_cost,
                'shrt_vwap': shrt_cost / shrt_amount if shrt_amount else 0.0,
                'long_start_ts': long_start_ts,
                'shrt_start_ts': shrt_start_ts,
                'last_long_entry_ts': last_long_entry_ts,
                'last_shrt_entry_ts': last_shrt_entry_ts,
                'last_long_entry_cost': last_long_entry_cost,
                'last_shrt_entry_cost': last_shrt_entry_cost,
                'entry_exit_amount_threshold': entry_exit_amount_threshold}

    start_ts = min(long_start_ts, shrt_start_ts) - 1000 * 60 * 60 * 24 * 3
    _, cropped_my_trades = partition_sorted(my_trades, lambda x: x['timestamp'] >= start_ts)
    return cropped_my_trades, analysis


def filter_orders(actual_orders: [dict], ideal_orders: [dict]) -> ([dict], [dict]):
    # actual_orders = [{'price': float, 'amount': float', 'side': str, 'id': int, ...}]
    # ideal_orders = [{'price': float, 'amount': float', 'side': str}]
    if not actual_orders:
        return [], ideal_orders
    if not ideal_orders:
        return actual_orders, []
    orders_to_delete = []
    ideal_orders_copy = [{k: o[k] for k in ['symbol', 'side', 'amount', 'price']}
                         for o in ideal_orders]
    for o in actual_orders:
        o_cropped = {k: o[k] for k in ['symbol', 'side', 'amount', 'price']}
        if o_cropped in ideal_orders_copy:
            ideal_orders_copy.remove(o_cropped)
        else:
            orders_to_delete.append(o)
    return orders_to_delete, ideal_orders_copy


async def main():
    settings = load_settings(sys.argv[1])
    bot = await create_bot(settings)
    await bot.start()


if __name__ == '__main__':
    asyncio.run(main())
