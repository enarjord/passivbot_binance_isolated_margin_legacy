from __future__ import annotations
import asyncio
import json
import websockets
import os
import sys
import random
import numpy as np
import traceback
from common_procedures import print_, load_key_secret, make_get_filepath
from common_functions import ts_to_date, calc_new_ema, remove_duplicates, partition_sorted, \
    round_up, round_dn, flatten
from time import time, sleep
from typing import Callable
from collections import defaultdict
from math import log10

import ccxt.async_support as ccxt_async


HOUR_TO_MILLIS = 60 * 60 * 1000
FORCE_UPDATE_INTERVAL_SECONDS = 60 * 4
MAX_ORDERS_PER_24H = 200000
MAX_ORDERS_PER_10M = MAX_ORDERS_PER_24H / 24 / 6
MAX_ORDERS_PER_10S = 100
DEFAULT_TIMEOUT = 10


def load_settings(user: str):
    try:
        settings = json.load(open(f'settings/binance_isolated_margin/{user}.json'))
    except:
        print_([f'user {user} not found, using default settings'])
        settings = json.load(open('settings/binance_isolated_margin/default.json'))
    settings['user'] = user
    return settings

async def tw(fn: Callable, args: tuple = (), kwargs: dict = {}):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        track = traceback.format_exc()
        print(f'error with {fn.__name__}', e, track, args, kwargs)
        return e


async def create_bot(settings: dict):
    bot = Bot(settings)
    await bot._init()
    return bot

class Bot:
    def __init__(self, settings: dict):
        self.user = settings['user']
        self.cc = ccxt_async.binance(
            {'apiKey': (ks := load_key_secret('binance', self.user))[0],
             'secret': ks[1]}
        )
        self.settings = settings['symbols']
        self.symbols = set(self.settings)
        self.balance = {}
        self.open_orders = {}
        self.my_bids = {}
        self.my_asks = {}
        self.my_trades = {}
        self.my_trades_analysis = {}

        self.rolling_10s_orders = []
        self.rolling_10m_orders = []

        self.emas = defaultdict(dict)
        self.min_ema = {}
        self.max_ema = {}
        self.ema_second = {}
        self.last_price = {}

        self.limits = defaultdict(dict)

    async def _init(self):
        for s in self.symbols:
            self.settings[s]['max_memory_span_millis'] = \
                self.settings[s]['max_memory_span_days'] * 24 * 60 * 60 * 1000
            self.settings[s]['ema_spans_minutes'] = sorted(self.settings[s]['ema_spans_minutes'])
            self.settings[s]['ema_spans_seconds'] = \
                [span * 60 for span in self.settings[s]['ema_spans_minutes']]
        self.balance_log_filepath = \
            make_get_filepath(f"logs/binance/{self.user}/balance/")
        await self.cc.load_markets()
        self.symbol_split = {s: s.split('/') for s in self.cc.markets}
        self.all_coins = set(flatten(self.symbol_split.values()))
        self.nodash_to_dash = {s.replace('/', ''): s for s in self.symbol_split}
        self.dash_to_nodash = {s: s.replace('/', '') for s in self.symbol_split}
        self.amount_precisions = {s: self.cc.markets[s]['precision']['amount']
                                  for s in self.cc.markets}
        self.price_precisions = {s: self.cc.markets[s]['precision']['price']
                                 for s in self.cc.markets}
        self.min_trade_costs = {s: self.cc.markets[s]['limits']['cost']['min']
                                for s in self.symbols}
        self.s2c = {s: self.symbol_split[s][0] for s in self.symbol_split}
        self.quot = self.symbol_split[next(iter(self.symbols))][1]
        assert all(self.symbol_split[s][1] == self.quot for s in self.symbols)

        now_m2 = time() - 2
        scs = flatten([[s + c for c in self.symbol_split[s]] for s in self.symbols])
        self.timestamps = {'locked': {'update_balance': {s: now_m2 for s in self.symbols},
                                      'update_open_orders': {s: now_m2 for s in self.symbols},
                                      'update_borrowable': {sc: now_m2 for sc in scs},
                                      'update_my_trades': {s: now_m2 for s in self.symbols},
                                      'create_bid': {s: now_m2 for s in self.symbols},
                                      'create_ask': {s: now_m2 for s in self.symbols},
                                      'cancel_order': {s: now_m2 for s in self.symbols},
                                      'borrow': {sc: now_m2 for sc in scs},
                                      'repay': {sc: now_m2 for sc in scs},
                                      'dump_balance_log': {s: 0 for s in self.symbols},
                                      'execute_to_exchange': {s: now_m2 for s in self.symbols}}}
        self.timestamps['released'] = {k0: {k1: self.timestamps['locked'][k0][k1] + 1
                                            for k1 in self.timestamps['locked'][k0]}
                                       for k0 in self.timestamps['locked']}

        await self.update_balance()
        for s in list(self.symbols):
            '''
            if s not in self.balance:
                result = await tw(self.enable_isolated, args=(s,))
                print_([result])
                sleep(2.0)
            '''
        await self.update_balance()
        for s in list(self.symbols):
            if s not in self.balance or not self.balance[s]['equity']:
                self.symbols.remove(s)

        self.longs = {s: self.settings[s]['long'] for s in self.symbols}
        self.shrts = {s: self.settings[s]['shrt'] for s in self.symbols}
        self.ideal_orders = \
            {s: {'shrt_sel': {'symbol': s, 'side': 'sel', 'amount': 0.0, 'price': 0.0},
                 'shrt_buy': {'symbol': s, 'side': 'buy', 'amount': 0.0, 'price': 0.0},
                 'long_buy': {'symbol': s, 'side': 'buy', 'amount': 0.0, 'price': 0.0},
                 'long_sel': {'symbol': s, 'side': 'sel', 'amount': 0.0, 'price': 0.0},}
             for s in self.symbols}

        self.depth_levels = 5
        self.symbol_formatting_map = {
            s.replace('/', '').lower() + f'@depth{self.depth_levels}': s
            for s in self.symbols
        }
        self.order_book = {s: {'s': s, 'bids': [], 'asks': []} for s in self.symbols}
        self.prev_order_book = self.order_book.copy()
        self.stream_tick_ts = 0
        self.conversion_costs = defaultdict(dict)

        await asyncio.gather(*[self.init_ema(s) for s in self.symbols])
        await asyncio.gather(*flatten([[self.update_borrowable(s, self.s2c[s]),
                                        self.update_borrowable(s, self.quot)]
                                       for s in self.symbols]))
        await asyncio.gather(*[self.update_my_trades(s) for s in self.symbols])
        await asyncio.gather(*[self.update_open_orders(s) for s in self.symbols])
        for s in self.symbols:
            lpincr = round(self.last_price[s] + 10**-self.price_precisions[s],
                           self.price_precisions[s])
            if lpincr / self.last_price[s] < 1.0001:
                print_([f'price precision for {s} too high. adjusting from',
                        f'{self.price_precisions[s]} to {self.price_precisions[s] - 1}'])
                self.price_precisions[s] -= 1
        print_(['finished init'])
        return

    def is_executing(self, key0, key1,
                     timeout: int = DEFAULT_TIMEOUT,
                     do_lock: bool = True) -> bool:
        now = time()
        if self.timestamps['released'][key0][key1] > self.timestamps['locked'][key0][key1] or \
                now - self.timestamps['locked'][key0][key1] > timeout:
            if do_lock:
                self.timestamps['locked'][key0][key1] = now
            return False
        # print_(['still executing', key0, key1])
        return True

    async def enable_isolated(self, symbol: str):
        print_(['enabling isolated margin account', symbol])
        coin, quot = self.symbol_split[symbol]
        return await tw(self.cc.sapi_post_margin_isolated_create, kwargs={'params': {
            'quote': quot,
            'base': coin
        }})

    async def transfer_to_isolated(self, symbol: str, coin: str, amount: float):
        # margin isolated transfer
        t = await tw(self.cc.sapi_post_margin_isolated_transfer, kwargs={'params': {
            'transTo': "ISOLATED_MARGIN",
            'amount': amount,
            'transFrom': "SPOT",
            'asset': coin,
            'symbol': symbol.replace('/', '')
        }})
        return t

    async def transfer_from_isolated(self, symbol: str, coin: str, amount: float):
        # margin isolated transfer
        t = await tw(self.cc.sapi_post_margin_isolated_transfer, kwargs={'params': {
            'transTo': "SPOT",
            'amount': amount,
            'transFrom': "ISOLATED_MARGIN",
            'asset': coin,
            'symbol': symbol.replace('/', '')
        }})
        return t

    async def create_bid(self, symbol: str, amount: float, price: float):
        if self.is_executing('create_bid', symbol):
            return
        coin, quot = self.symbol_split[symbol]
        side_effect = 'MARGIN_BUY' if self.balance[symbol][quot]['free'] < amount * price \
            else 'AUTO_REPAY'
        bid = await tw(self.cc.sapi_post_margin_order, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE',
            'side': 'BUY',
            'type': 'LIMIT',
            'quantity': amount,
            'price': price,
            'sideEffectType': side_effect
        }})
        if type(bid) != dict:
            bidf = None
            asyncio.create_task(tw(self.update_balance, args=(symbol,)))
            print_(['error creating bid', symbol, amount, price])

        else:
            bidf = {
                    'symbol': symbol,
                    'order_id': int(bid['orderId']),
                    'client_order_id': bid['clientOrderId'],
                    'price': float(bid['price']),
                    'amount': float(bid['origQty']),
                    'executed_amount': float(bid['executedQty']),
                    'status': bid['status'],
                    'type': bid['type'],
                    'side': bid['side'].lower(),
                    'fills': bid['fills']
                }
            bidf['margin_buy_borrow_coin'] = bid['marginBuyBorrowAsset'] \
                if 'marginBuyBorrowAsset' in bid else ''
            bidf['margin_buy_borrow_amount'] = float(bid['marginBuyBorrowAmount']) \
                if 'marginBuyBorrowAmount' in bid else 0.0
            self.open_orders[symbol] = \
                sorted(self.open_orders[symbol] + [bidf], key=lambda x: x['price'])
            self.my_bids[symbol] = sorted(self.my_bids[symbol] + [bidf], key=lambda x: x['price'])
            self.balance[symbol][quot]['free'] -= bidf['amount'] * bidf['price']
            self.balance[symbol][quot]['used'] += bidf['amount'] * bidf['price']
            print_(['  created',
                    [bidf[k] for k in ['symbol', 'side', 'amount', 'price', 'order_id']]])
        self.timestamps['released']['create_bid'][symbol] = time()
        return bidf

    async def create_ask(self, symbol: str, amount: float, price: float):
        if self.is_executing('create_ask', symbol):
            return
        coin, quot = self.symbol_split[symbol]
        side_effect = 'MARGIN_BUY' if self.balance[symbol][coin]['free'] < amount else 'AUTO_REPAY'
        ask = await tw(self.cc.sapi_post_margin_order, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE',
            'side': 'SELL',
            'type': 'LIMIT',
            'quantity': amount,
            'price': price,
            'sideEffectType': side_effect
        }})
        if type(ask) != dict:
            askf = None
            asyncio.create_task(tw(self.update_balance, args=(symbol,)))
            print_(['error creating ask', symbol, amount, price])
        else:
            askf = {
                    'symbol': symbol,
                    'order_id': int(ask['orderId']),
                    'client_order_id': ask['clientOrderId'],
                    'price': float(ask['price']),
                    'amount': float(ask['origQty']),
                    'executed_amount': float(ask['executedQty']),
                    'status': ask['status'],
                    'type': ask['type'],
                    'side': ask['side'].lower(),
                    'fills': ask['fills']
                }
            askf['margin_buy_borrow_coin'] = ask['marginBuyBorrowAsset'] \
                if 'marginBuyBorrowAsset' in ask else ''
            askf['margin_buy_borrow_amount'] = float(ask['marginBuyBorrowAmount']) \
                if 'marginBuyBorrowAmount' in ask else 0.0
            self.open_orders[symbol] = \
                sorted(self.open_orders[symbol] + [askf], key=lambda x: x['price'])
            self.my_asks[symbol] = sorted(self.my_asks[symbol] + [askf], key=lambda x: x['price'])
            self.balance[symbol][coin]['free'] -= askf['amount']
            self.balance[symbol][coin]['used'] += askf['amount']
            print_(['  created',
                    [askf[k] for k in ['symbol', 'side', 'amount', 'price', 'order_id']]])
    
        self.timestamps['released']['create_ask'][symbol] = time()
        return askf

    async def cancel_order(self, symbol: str, id_: int):
        if self.is_executing('cancel_order', symbol):
            return
        cancelled = await tw(self.cc.sapi_delete_margin_order, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE',
            'orderId': int(id_)
        }})
        if type(cancelled) != dict:
            cf = None
            try:
                if '-2011' in cancelled.args[0]:
                    print_(['error cancelling order, code -2011, unknown order', symbol, id_])
                    asyncio.create_task(self.update_open_orders(symbol))
            except:
                pass
        else:
            cf = {
                    'symbol': symbol,
                    'order_id': int(cancelled['orderId']),
                    'orig_client_order_id': cancelled['origClientOrderId'],
                    'client_order_id': cancelled['clientOrderId'],
                    'price': float(cancelled['price']),
                    'amount': float(cancelled['origQty']),
                    'executed_amount': float(cancelled['executedQty']),
                    'status': cancelled['status'],
                    'type': cancelled['type'],
                    'side': cancelled['side'].lower()
                }
            self.open_orders[symbol] = [o for o in self.open_orders[symbol]
                                        if o['order_id'] != cf['order_id']]
            if cf['side'] == 'buy':
                self.my_bids[symbol] = [o for o in self.my_bids[symbol]
                                        if o['order_id'] != cf['order_id']]
            elif cf['side'] == 'sell':
                self.my_asks[symbol] = [o for o in self.my_asks[symbol]
                                        if o['order_id'] != cf['order_id']]
            print_(['cancelled',
                    [cf[k] for k in ['symbol', 'side', 'amount', 'price', 'order_id']]])
        self.timestamps['released']['cancel_order'][symbol] = time()
        return cf

    async def borrow(self, symbol: str, coin: str, amount: float):
        sc = symbol + coin
        if self.is_executing('borrow', sc):
            return
        borrowed = await tw(self.cc.sapi_post_margin_loan, kwargs={'params': {
            'asset': coin,
            'isIsolated': 'TRUE',
            'symbol': symbol.replace('/', ''),
            'amount': amount
        }})
        self.timestamps['released']['borrow'][sc] = time()
        return {**{'symbol': symbol, 'coin': coin, 'amount': amount}, **borrowed}

    async def repay(self, symbol: str, coin: str, amount: float):
        sc = symbol + coin
        if self.is_executing('repay', sc):
            return
        repaid = await tw(self.cc.sapi_post_margin_repay, kwargs={'params': {
            'asset': coin,
            'isIsolated': 'TRUE',
            'symbol': symbol.replace('/', ''),
            'amount': amount
        }})
        self.timestamps['released']['repay'][sc] = time()
        return {**{'symbol': symbol, 'coin': coin, 'amount': amount}, **repaid}

    async def update_borrowable(self, symbol: str, coin: str):
        sc = symbol + coin
        if self.is_executing('update_borrowable', sc):
            return
        print_(['updating borrowable', symbol, coin])
        borrowable = await tw(self.get_borrowable, args=(symbol, coin))
        self.balance[symbol][coin]['borrowable'] = borrowable['amount']
        self.timestamps['released']['update_borrowable'][sc] = time()

    async def update_balance(self, symbol: str = None):
        if symbol is not None and self.is_executing('update_balance', symbol):
            return
        print_(['updating balance', symbol])
        fetched = await tw(self.get_balance, args=(symbol,))
        for e in fetched['assets']:
            s = self.nodash_to_dash[e['symbol']]
            if s not in self.symbols:
                continue
            coin, quot = self.symbol_split[s]
            balance = {
                k0: {
                    'free': (free := float(e[k1]['free'])),
                    'used': (used := float(e[k1]['locked'])),
                    'onhand': free + used,
                    'debt': float(e[k1]['borrowed']) + float(e['quoteAsset']['interest']),
                    'equity': float(e[k1]['netAsset']),
                    'equity_ito_btc': float(e[k1]['netAssetOfBtc']),
                    'borrowable': (self.balance[s][k0]['borrowable']
                                   if s in self.balance and 'borrowable' in self.balance[s][k0]
                                   else 0.0),
                } for k0, k1 in zip([quot, coin], ['quoteAsset', 'baseAsset'])
            }
            balance['equity'] = sum([balance[k]['equity_ito_btc'] for k in [coin, quot]])
            balance['entry_cost'] = max(
                self.settings[s]['account_equity_pct_per_entry'] * balance['equity'],
                self.min_trade_costs[s]
            )
            try:
                if self.balance[s][coin]['equity'] != balance[coin]['equity'] or \
                        self.balance[s][quot]['equity'] != balance[quot]['equity']:
                    await asyncio.gather(*[tw(self.update_open_orders, args=(s,)),
                                           tw(self.update_my_trades, args=(s,)),
                                           tw(self.update_borrowable, args=(s, coin)),
                                           tw(self.update_borrowable, args=(s, quot))])
            except Exception as e:
                pass

            self.balance[s] = balance
            self.dump_balance_log(s)
            self.timestamps['released']['update_balance'][s] = time()

    async def get_balance(self, symbol: str = None):
        if symbol is None:
            return await tw(self.cc.sapi_get_margin_isolated_account)
        return await tw(self.cc.sapi_get_margin_isolated_account,
                        kwargs={'params': {'symbols': self.dash_to_nodash[symbol]}})

    async def get_open_orders(self, symbol: str):
        oos = await tw(self.cc.sapi_get_margin_openorders, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE'
        }})
        oosf = []
        for o in oos:
            oof = {
                'symbol': symbol,
                'order_id': int(o['orderId']),
                'client_order_id': o['clientOrderId'],
                'price': float(o['price']),
                'amount': float(o['origQty']),
                'executed_amount': float(o['executedQty']),
                'status': o['status'],
                'type': o['type'],
                'side': o['side'].lower(),
                'timestamp': o['time']
            }
            oosf.append(oof)
        return oosf

    async def get_borrowable(self, symbol: str, coin: str):
        borrowable = await tw(self.cc.sapi_get_margin_maxborrowable, kwargs={'params': {
            'asset': coin,
            'isolatedSymbol': symbol.replace('/', '')
        }})
        return {'symbol': symbol, 'coin': coin, 'amount': float(borrowable['amount'])}

    async def update_open_orders(self, symbol):
        if self.is_executing('update_open_orders', symbol):
            return
        print_(['updating open_orders', symbol])
        oos = await tw(self.get_open_orders, args=(symbol,))
        self.open_orders[symbol] = []
        self.my_bids[symbol] = []
        self.my_asks[symbol] = []
        for o in sorted(oos, key=lambda x: x['price']):
            if o['side'] == 'buy':
                self.my_bids[symbol].append(o)
            elif o['side'] == 'sell':
                self.my_asks[symbol].append(o)
            self.open_orders[symbol].append(o)
        self.timestamps['released']['update_open_orders'][symbol] = time()

    async def fetch_my_trades(self, symbol: str, from_id: int = None) -> [dict]:
        no_dash = self.dash_to_nodash[symbol]
        cache_filepath = make_get_filepath(
            f"cache/binance/{self.user}/my_trades_margin_isolated/{no_dash}/")
        history = []
        ids_done = set()
        if from_id is None:
            for fname in [f for f in os.listdir(cache_filepath) if f.endswith('.txt')]:
                with open(cache_filepath + fname) as f:
                    for line in f.readlines():
                        mt = json.loads(line)
                        if mt['id'] in ids_done:
                            continue
                        history.append(mt)
                        ids_done.add(mt['id'])
            from_id = max(ids_done) + 1 if ids_done else 0
        limit = 100
        while True:
            my_trades = await tw(self.cc.sapi_get_margin_mytrades, kwargs={'params': {
                'symbol': no_dash,
                'isIsolated': 'TRUE',
                'fromId': from_id,
                'limit': limit
                }})
            if not my_trades:
                break
            print_(['fetched my trades', symbol, ts_to_date(my_trades[0]['time'] / 1000)])
            for int_id, mt in sorted([(int(mt_['id']), mt_) for mt_ in my_trades]):
                mtf = {
                    'symbol': symbol,
                    'id': int_id,
                    'order_id': int(mt['orderId']),
                    'side': 'buy' if mt['isBuyer'] else 'sell',
                    'amount': (amount := float(mt['qty'])),
                    'price': (price := float(mt['price'])),
                    'cost': round(amount * price, 12),
                    'fee': float(mt['commission']),
                    'fee_coin': mt['commissionAsset'],
                    'timestamp': mt['time'],
                    'datetime': ts_to_date(mt['time'] / 1000),
                    'is_maker': mt['isMaker']
                }

                from_id = mtf['id'] + 1
                if mtf['id'] in ids_done:
                    continue
                history.append(mtf)
                ids_done.add(mtf['id'])
                with open(f"{cache_filepath}{mtf['datetime'][:7]}.txt", 'a') as f:
                    f.write(json.dumps(mtf) + '\n')
        return sorted(history, key=lambda x: x['id'])

    async def update_my_trades(self, symbol: str) -> None:
        if self.is_executing('update_my_trades', symbol, timeout=120):
            return
        print_(['updating my trades', symbol])
        if symbol in self.my_trades:
            from_id = self.my_trades[symbol][-1]['id'] + 1 if self.my_trades[symbol] else 0
            fetched = await tw(self.fetch_my_trades,
                               args=(symbol,),
                               kwargs={'from_id': from_id})
            seen = set()
            mts = []
            for mt in sorted(self.my_trades[symbol] + fetched, key=lambda x: x['id']):
                if mt['id'] in seen:
                    continue
                mts.append(mt)
                seen.add(mt['id'])
        else:
            mts = await tw(self.fetch_my_trades, args=(symbol,))
        age_limit_millis = max(
            self.cc.milliseconds() - self.settings[symbol]['max_memory_span_millis'],
            self.settings[symbol]['snapshot_timestamp_millis']
        )
        entry_exit_amount_threshold = (self.balance[symbol]['entry_cost'] *
                                       self.settings[symbol]['min_exit_cost_multiplier'] *
                                       0.75 /
                                       self.last_price[symbol])
        my_trades, analysis = \
            analyze_my_trades([e for e in mts if e['timestamp'] > age_limit_millis],
                              entry_exit_amount_threshold)
        c, q = self.symbol_split[symbol]
        bag_ratio = ((analysis['long_cost'] - analysis['shrt_cost']) /
                     self.balance[symbol]['equity'])
        bag_ratio_m = bag_ratio * self.settings[symbol]['profit_pct_multiplier']
        analysis['long_sel_price'] = round_up(
            (1 + min(self.settings[symbol]['max_profit_pct'],
                     max(self.settings[symbol]['min_profit_pct'],
                         -bag_ratio_m))) * analysis['long_vwap'],
            self.price_precisions[symbol]
        )
        analysis['shrt_buy_price'] = round_dn(
            (1 - min(self.settings[symbol]['max_profit_pct'],
                     max(self.settings[symbol]['min_profit_pct'],
                         bag_ratio_m))) * analysis['shrt_vwap'],
            self.price_precisions[symbol]
        )
        self.my_trades_analysis[symbol] = analysis
        self.my_trades[symbol] = my_trades
        self.timestamps['released']['update_my_trades'][symbol] = time()

    def on_update(self, s: str):
        if self.order_book[s]['bids'][-1]['price'] != self.prev_order_book[s]['bids'][-1]['price']:
            # highest bid changed
            if self.order_book[s]['bids'][-1]['price'] < \
                    self.prev_order_book[s]['bids'][-1]['price']:
                # highest bid became lower
                if self.my_bids[s]:
                    if self.order_book[s]['bids'][-1]['price'] < self.my_bids[s][-1]['price']:
                        print_(['bid taken', s])
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        asyncio.create_task(tw(self.update_open_orders, args=(s,)))
                    elif self.order_book[s]['bids'][-1]['price'] == self.my_bids[s][-1]['price']:
                        print_(['bid maybe taken', s])
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
        if self.order_book[s]['asks'][0]['price'] != self.prev_order_book[s]['asks'][0]['price']:
            # lowest ask changed
            if self.order_book[s]['asks'][0]['price'] > \
                    self.prev_order_book[s]['asks'][0]['price']:
                # lowest ask became higher
                if self.my_asks[s]:
                    if self.order_book[s]['asks'][0]['price'] > self.my_asks[s][0]['price']:
                        print_(['ask taken', s])
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        asyncio.create_task(tw(self.update_open_orders, args=(s,)))
                    elif self.order_book[s]['asks'][0]['price'] == self.my_asks[s][0]['price']:
                        print_(['ask maybe taken', s])
                        asyncio.create_task(tw(self.update_balance, args=(s,)))

        now = time()
        for key0 in ['update_balance', 'update_open_orders', 'update_my_trades']:
            if now - self.timestamps['released'][key0][s] > \
                    FORCE_UPDATE_INTERVAL_SECONDS + np.random.choice(np.arange(-60, 60, 1)):
                print_(['force', key0, s])
                asyncio.create_task(tw(getattr(self, key0), args=(s,)))
        for k in self.symbol_split[s]:
            if now - self.timestamps['released']['update_borrowable'][s + k] > \
                    FORCE_UPDATE_INTERVAL_SECONDS + np.random.choice(np.arange(-60, 60, 1)):
                print_(['force update borrowable', s, k])
                asyncio.create_task(tw(self.update_borrowable, args=(s, k)))

        asyncio.create_task(tw(self.execute_to_exchange, args=(s,)))

    async def execute_to_exchange(self, s: str) -> None:
        now = time()
        if now - self.timestamps['released']['execute_to_exchange'][s] < 0.5:
            return
        if self.is_executing('execute_to_exchange', s):
            return
        for key0 in ['update_balance', 'update_open_orders', 'update_my_trades']:
            if self.is_executing(key0, s, do_lock=False):
                return
        c, q = self.symbol_split[s]
        for k in [c, q]:
            if self.is_executing('update_borrowable', s + k, do_lock=False):
                return
        self.set_ideal_orders(s)
        eligible_orders = self.allocate_credit(s)
        orders_to_delete, orders_to_create = filter_orders(self.open_orders[s], eligible_orders)
        for o in orders_to_delete[:4]:
            asyncio.create_task(tw(self.cancel_order, args=(s, o['order_id'])))
            await asyncio.sleep(0.1)
        for o in orders_to_create[:2]:
            if o['side'] == 'buy' and len(self.my_bids[s]) < 2:
                asyncio.create_task(tw(self.create_bid, args=(s, o['amount'], o['price'])))
                await asyncio.sleep(0.1)
            elif o['side'] == 'sell' and len(self.my_asks[s]) < 2:
                asyncio.create_task(tw(self.create_ask, args=(s, o['amount'], o['price'])))
                await asyncio.sleep(0.1)
        self.timestamps['released']['execute_to_exchange'][s] = time()



    async def start_websocket(self):
        print_([self.symbol_formatting_map])
        uri = "wss://stream.binance.com:9443/stream?streams=" + '/'.join(
            list(self.symbol_formatting_map))
        print_([uri])
        print_(['longing', sorted(self.longs)])
        print_(['shrting', sorted(self.shrts)])
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
                bid_ask_mid = (ob['bids'][-1]['price'] + ob['asks'][0]['price']) / 2
                if now_second > self.ema_second[symbol]:
                    for span_s, span_m in zip(self.settings[symbol]['ema_spans_seconds'],
                                              self.settings[symbol]['ema_spans_minutes']):
                        self.emas[symbol][span_m] = calc_new_ema(
                            self.last_price[symbol],
                            bid_ask_mid,
                            self.emas[symbol][span_m],
                            span=span_s,
                            n_steps=now_second - self.ema_second[symbol]
                        )
                    self.min_ema[symbol] = min(self.emas[symbol].values())
                    self.max_ema[symbol] = max(self.emas[symbol].values())
                    self.ema_second[symbol] = now_second
                self.prev_order_book[symbol] = self.order_book[symbol]
                self.order_book[symbol] = ob
                self.last_price[symbol] = bid_ask_mid
                self.conversion_costs[coin][quot] = ob['bids'][-1]['price']
                self.conversion_costs[quot][coin] = 1.0 / ob['asks'][0]['price']
                self.stream_tick_ts = time()
                self.on_update(symbol)

    def stop(self):
        self.listener.cancel()
        print_(['bot stopped'])

    async def start(self, do_wait: bool = True):
        if do_wait:
            self.listener = await self.start_websocket()
        else:
            self.listener = asyncio.create_task(self.start_websocket())

    async def init_ema(self, symbol: str):
        print_(['initiating ema', symbol])
        closes = [e[4] for e in await tw(self.cc.fetch_ohlcv,
                                         args=(symbol,),
                                         kwargs={'limit': 1000})]
        for span in self.settings[symbol]['ema_spans_minutes']:
            ema = closes[0]
            alpha = 2 / (span + 1)
            alpha_ = 1 - alpha
            for close in closes:
                ema = ema * alpha_ + close * alpha
            self.emas[symbol][span] = ema
        self.min_ema[symbol] = min(self.emas[symbol].values())
        self.max_ema[symbol] = max(self.emas[symbol].values())
        self.ema_second[symbol] = int(time())
        self.last_price[symbol] = closes[-1]
        coin, quot = self.symbol_split[symbol]
        self.conversion_costs[coin][quot] = closes[-1]
        self.conversion_costs[quot][coin] = 1.0 / closes[-1]
        self.order_book[symbol] = {'bids': [{'price': closes[-1], 'amount': 0.0}],
                                   'asks': [{'price': closes[-1], 'amount': 0.0}]}

    def convert_amount(self, amount: float, coin_from: str, coin_to: str):
        try:
            return amount * self.conversion_costs[coin_from][coin_to]
        except KeyError:
            if coin_from == coin_to:
                return amount
            return (amount * self.conversion_costs[coin_from]['BTC'] *
                    self.conversion_costs['BTC'][coin_to])

    def consume_quota(self):
        now = time()
        self.rolling_10s_orders.append(now)
        self.rolling_10m_orders.append(now)

    def set_ideal_orders(self, s: str):

        coin, quot = self.symbol_split[s]

        # prepare data
        other_bids = calc_other_orders(self.my_bids[s], self.order_book[s]['bids'],
                                       self.price_precisions[s])
        other_asks = calc_other_orders(self.my_asks[s], self.order_book[s]['asks'],
                                       self.price_precisions[s])

        highest_other_bid = sorted(other_bids, key=lambda x: x['price'])[-1] \
            if other_bids else self.my_bids[s][-1]
        lowest_other_ask = sorted(other_asks, key=lambda x: x['price'])[0] \
            if other_asks else self.my_asks[s][0]

        other_bid_incr = round(highest_other_bid['price'] + 10**-self.price_precisions[s],
                               self.price_precisions[s])
        other_ask_decr = round(lowest_other_ask['price'] - 10**-self.price_precisions[s],
                               self.price_precisions[s])

        entry_cost = self.balance[s]['entry_cost']
        min_exit_cost = entry_cost * self.settings[s]['min_exit_cost_multiplier']

        # set ideal orders
        exponent = self.settings[s]['entry_vol_modifier_exponent']
        now_millis = self.cc.milliseconds()
        if self.settings[s]['shrt']:
            shrt_sel_price = max([
                round_up(self.max_ema[s] * (1 + self.settings[s]['entry_spread']),
                         self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr if entry_cost / other_ask_decr < lowest_other_ask['amount']
                 else lowest_other_ask['price'])
            ])
            shrt_amount_modifier = max(
                1.0,
                min(
                    self.settings[s]['min_exit_cost_multiplier'] / 2,
                    (self.last_price[s] /
                     self.my_trades_analysis[s]['shrt_vwap'])**exponent
                )
            ) if self.my_trades_analysis[s]['shrt_vwap'] > 0.0 else 1.0
            delay_hours = min(self.my_trades_analysis[s]['last_shrt_entry_cost'], entry_cost) / \
                (self.settings[s]['account_equity_pct_per_hour'] * self.balance[s]['equity'])
            if now_millis - self.my_trades_analysis[s]['last_shrt_entry_ts'] > \
                    (HOUR_TO_MILLIS * delay_hours):
                shrt_sel_amount = round_up(entry_cost * shrt_amount_modifier / shrt_sel_price,
                                           self.amount_precisions[s])
            else:
                shrt_sel_amount = 0.0
            if shrt_sel_amount * shrt_sel_price >= self.min_trade_costs[s]:
                if shrt_sel_price != self.ideal_orders[s]['shrt_sel']['price'] or \
                        (round(shrt_sel_amount - 10**-self.amount_precisions[s],
                               self.amount_precisions[s]) !=
                         self.ideal_orders[s]['shrt_sel']['amount']):
                    self.ideal_orders[s]['shrt_sel'] = {
                        'symbol': s, 'side': 'sell',
                        'amount': shrt_sel_amount,
                        'price': shrt_sel_price
                    }
            else:
                self.ideal_orders[s]['shrt_sel'] = {
                    'symbol': s, 'side': 'sell',
                    'amount': 0.0,
                    'price': shrt_sel_price
                }
            shrt_buy_amount = round_up(self.my_trades_analysis[s]['shrt_amount'],
                                       self.amount_precisions[s])
            shrt_buy_price =  min([round_dn(self.min_ema[s], self.price_precisions[s]),
                                   other_ask_decr,
                                   (other_bid_incr if shrt_buy_amount < highest_other_bid['amount']
                                    else highest_other_bid['price']),
                                   self.my_trades_analysis[s]['shrt_buy_price']])
            if shrt_buy_amount * shrt_buy_price > min_exit_cost:
                self.ideal_orders[s]['shrt_buy'] = {'symbol': s, 'side': 'buy',
                                                    'amount': shrt_buy_amount,
                                                    'price': shrt_buy_price}
            else:
                self.ideal_orders[s]['shrt_buy'] = {'symbol': s, 'side': 'buy',
                                                    'amount': 0.0,
                                                    'price': shrt_buy_price}
        else:
            self.ideal_orders[s]['shrt_sel'] = {
                'symbol': s, 'side': 'sell',
                'amount': 0.0,
                'price': 0.0
            }
            self.ideal_orders[s]['shrt_buy'] = {
                'symbol': s, 'side': 'buy',
                'amount': 0.0,
                'price': 0.0
            }
        if self.settings[s]['long']:
            long_buy_price = min([
                round_dn(self.min_ema[s] * (1 - self.settings[s]['entry_spread']),
                         self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr if entry_cost / other_bid_incr < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            long_amount_modifier = max(
                1.0,
                min(
                    self.settings[s]['min_exit_cost_multiplier'] / 2,
                    (self.my_trades_analysis[s]['long_vwap'] /
                     self.last_price[s])**exponent
                )
            )
            delay_hours = min(self.my_trades_analysis[s]['last_long_entry_cost'], entry_cost) / \
                (self.settings[s]['account_equity_pct_per_hour'] * self.balance[s]['equity'])
            if now_millis - self.my_trades_analysis[s]['last_long_entry_ts'] > \
                    (HOUR_TO_MILLIS * delay_hours):
                long_buy_amount = round_up(entry_cost * long_amount_modifier / long_buy_price,
                                           self.amount_precisions[s])
            else:
                long_buy_amount = 0.0
            if long_buy_price * long_buy_amount >= self.min_trade_costs[s]:
                if long_buy_price != self.ideal_orders[s]['long_buy']['price'] or \
                        (round(long_buy_amount - 10**-self.amount_precisions[s],
                               self.amount_precisions[s]) !=
                         self.ideal_orders[s]['long_buy']['amount']):
                    self.ideal_orders[s]['long_buy'] = {
                        'symbol': s, 'side': 'buy',
                        'amount': long_buy_amount,
                        'price': long_buy_price
                    }
            else:
                self.ideal_orders[s]['long_buy'] = {
                    'symbol': s, 'side': 'buy',
                    'amount': 0.0,
                    'price': long_buy_price
                }
            long_sel_amount = round_up(self.my_trades_analysis[s]['long_amount'],
                                       self.amount_precisions[s])
            long_sel_price = max([round_up(self.max_ema[s], self.price_precisions[s]),
                                  other_bid_incr,
                                  (other_ask_decr if long_sel_amount < lowest_other_ask['amount']
                                   else lowest_other_ask['price']),
                                  self.my_trades_analysis[s]['long_sel_price']])
            if long_sel_amount * long_sel_price > min_exit_cost:
                self.ideal_orders[s]['long_sel'] = {'symbol': s, 'side': 'sell',
                                                    'amount': long_sel_amount,
                                                    'price': long_sel_price}
            else:
                self.ideal_orders[s]['long_sel'] = {'symbol': s, 'side': 'sell',
                                                    'amount': 0.0,
                                                    'price': long_sel_price}
        else:
            self.ideal_orders[s]['long_buy'] = {
                'symbol': s, 'side': 'buy',
                'amount': 0.0,
                'price': 0.0
            }
            self.ideal_orders[s]['long_sel'] = {
                'symbol': s, 'side': 'sell',
                'amount': 0.0,
                'price': 0.0
            }
        return

    def allocate_credit(self, s: str):
        c, q = self.symbol_split[s]
        coin_available = self.balance[s][c]['onhand'] + self.balance[s][c]['borrowable']
        quot_available = self.balance[s][q]['onhand'] + self.balance[s][q]['borrowable']
        min_exit_cost = self.balance[s]['entry_cost'] * self.settings[s]['min_exit_cost_multiplier']
        eligible_orders = []
        o = self.ideal_orders[s]['long_buy']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if quot_available >= cost:
                eligible_orders.append(o)
                quot_available -= cost
            else:
                new_amount = round_dn(quot_available / o['price'], self.price_precisions[s])
                if (new_cost := new_amount * o['price']) > self.min_trade_costs[s]:
                    o['amount'] = new_amount
                    eligible_orders.append(o)
                    quot_available -= new_cost
        o = self.ideal_orders[s]['shrt_sel']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if coin_available >= o['amount']:
                eligible_orders.append(o)
                coin_available -= o['amount']
            else:
                new_amount = round_dn(coin_available, self.price_precisions[s])
                if new_amount * o['price'] > self.min_trade_costs[s]:
                    o['amount'] = new_amount
                    eligible_orders.append(o)
                    coin_available -= new_amount
        o = self.ideal_orders[s]['long_sel']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if coin_available >= o['amount']:
                eligible_orders.append(o)
                coin_available -= o['amount']
            else:
                new_amount = round_dn(coin_available, self.price_precisions[s])
                if new_amount * o['price'] > min_exit_cost:
                    o['amount'] = new_amount
                    eligible_orders.append(o)
                    coin_available -= new_amount
        o = self.ideal_orders[s]['shrt_buy']
        if (cost := o['price'] * o['amount']) > min_exit_cost:
            if quot_available >= cost:
                eligible_orders.append(o)
                quot_available -= cost
            else:
                new_amount = round_dn(quot_available / o['price'], self.price_precisions[s])
                if (new_cost := new_amount * o['price']) > min_exit_cost:
                    o['amount'] = new_amount
                    eligible_orders.append(o)
                    quot_available -= new_cost
        return eligible_orders


    def dump_balance_log(self, s: str):
        now = time()
        if now - self.timestamps['locked']['dump_balance_log'][s] < 60 * 60:
            return
        print_(['dumping balance', s])
        filepath = make_get_filepath(
            f'logs/binance/{self.user}/isolated_margin_balances/{self.dash_to_nodash[s]}.txt'
        )
        with open(filepath, 'a') as f:
            line = json.dumps({**{'timestamp': self.cc.milliseconds()}, **self.balance[s]}) + '\n'
            f.write(line)
        self.timestamps['locked']['dump_balance_log'][s] = now


####################################################################################################
####################################################################################################
####################################################################################################


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

    start_ts = min(long_start_ts, shrt_start_ts) - 1000 * 60 * 60 * 24 * 30
    _, cropped_my_trades = partition_sorted(my_trades, lambda x: x['timestamp'] >= start_ts)
    return cropped_my_trades, analysis


def filter_orders(actual_orders: [dict], ideal_orders: [dict]) -> ([dict], [dict]):

    keys = ['symbol', 'side', 'amount', 'price']
    # actual_orders = [{'price': float, 'amount': float', 'side': str, 'id': int, ...}]
    # ideal_orders = [{'price': float, 'amount': float', 'side': str}]
    if not actual_orders:
        return [], ideal_orders
    if not ideal_orders:
        return actual_orders, []
    orders_to_delete = []
    orders_to_create = []
    ideal_orders_cropped = [{k: o[k] for k in keys} for o in ideal_orders]
    actual_orders_cropped = [{k: o[k] for k in keys} for o in actual_orders]

    for oc, o in zip(actual_orders_cropped, actual_orders):
        if oc not in ideal_orders_cropped:
            orders_to_delete.append(o)
            # ideal_orders_cropped = [io for io in ideal_orders_cropped if io != oc]
    for oc, o in zip(ideal_orders_cropped, ideal_orders):
        if oc not in actual_orders_cropped:
            orders_to_create.append(o)
            actual_orders_cropped.append(oc)
    return orders_to_delete, orders_to_create


async def main():
    settings = load_settings(sys.argv[1])
    bot = await create_bot(settings)
    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.cc.close()


if __name__ == '__main__':
    asyncio.run(main())
