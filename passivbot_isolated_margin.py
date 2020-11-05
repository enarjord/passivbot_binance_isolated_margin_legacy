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
DAY_TO_MILLIS = HOUR_TO_MILLIS * 24
FORCE_UPDATE_INTERVAL_SECONDS = 60 * 4
MAX_ORDERS_PER_24H = 200000
MAX_ORDERS_PER_10M = MAX_ORDERS_PER_24H / 24 / 6
MAX_ORDERS_PER_10S = 100
DEFAULT_TIMEOUT = 10


def load_settings(user: str):
    try:
        settings = json.load(open(f'settings/binance_isolated_margin/{user}.json'))
    except Exception as e:
        print(e)
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
        self.global_settings = settings['global']
        self.default_settings = settings['default']
        self.symbols = set(self.settings)

        self.balance = {}
        self.open_orders = {}
        self.my_bids = {}
        self.my_asks = {}
        self.my_trades = {}
        self.my_trades_analysis = {}
        self.sum_past_hour_long_entry_vol = 0.0
        self.sum_past_hour_shrt_entry_vol = 0.0

        self.rolling_10s_orders = []
        self.rolling_10m_orders = []

        self.emas = defaultdict(dict)
        self.min_ema = {}
        self.max_ema = {}
        self.ema_second = {}
        self.last_price = {}
        self.last_my_trades_id = {}

        self.limits = defaultdict(dict)

    async def _init(self):
        self.active_symbols = []
        for s in self.symbols:
            for k in self.default_settings:
                if k not in self.settings[s]:
                    self.settings[s][k] = self.default_settings[k]
            self.settings[s]['max_memory_span_millis'] = \
                self.settings[s]['max_memory_span_days'] * 24 * 60 * 60 * 1000
            self.settings[s]['ema_spans_minutes'] = sorted(self.settings[s]['ema_spans_minutes'])
            self.settings[s]['ema_spans_seconds'] = \
                [span * 60 for span in self.settings[s]['ema_spans_minutes']]
            if self.settings[s]['long'] or self.settings[s]['shrt']:
                self.active_symbols.append(s)
        for s in self.symbols:
            self.settings[s]['account_equity_pct_per_hour'] = \
                (self.global_settings['max_entry_acc_val_pct_per_hour'] /
                 len(self.active_symbols) * 2)
            self.settings[s]['account_equity_pct_per_entry'] = \
                (self.settings[s]['account_equity_pct_per_hour'] *
                 self.settings[s]['min_entry_delay_hours']) / 2
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
        self.s2q = {s: self.symbol_split[s][1] for s in self.symbol_split}
        self.quot = self.symbol_split[next(iter(self.symbols))][1]
        assert all(self.symbol_split[s][1] == self.quot for s in self.symbols)

        now_m2 = time() - 2
        scs = flatten([[s + c for c in self.symbol_split[s]] for s in self.symbols])
        self.timestamps = {'locked': {'update_balance': {s: now_m2
                                                         for s in list(self.symbols) + ['all']},
                                      'update_open_orders': {s: now_m2 for s in self.symbols},
                                      'update_borrowable': {sc: now_m2 for sc in scs},
                                      'update_my_trades': {s: now_m2 for s in self.symbols},
                                      'create_bid': {s: now_m2 for s in self.symbols},
                                      'create_ask': {s: now_m2 for s in self.symbols},
                                      'cancel_order': {s: now_m2 for s in self.symbols},
                                      'borrow': {sc: now_m2 for sc in scs},
                                      'repay': {sc: 0 for sc in scs},
                                      'dump_balance_log': {'dump_balance_log': 0},
                                      'distribute_btc': {'distribute_btc': now_m2 - 60 * 9},
                                      'execute_to_exchange': {s: now_m2 for s in self.symbols}}}
        self.timestamps['released'] = {k0: {k1: self.timestamps['locked'][k0][k1] + 1
                                            for k1 in self.timestamps['locked'][k0]}
                                       for k0 in self.timestamps['locked']}
        tickers = await self.cc.fetch_tickers()
        self.last_price = {s_: tickers[s_]['last'] for s_ in tickers if tickers[s_]['last'] > 0.0}
        self.conversion_costs = defaultdict(dict)
        self.order_book = {s: {'s': s, 'bids': [], 'asks': []} for s in self.symbols}
        self.prev_order_book = self.order_book.copy()
        await asyncio.gather(*[self.init_ema(s) for s in self.symbols])

        await self.update_balance()
        for s in list(self.symbols):
            if s not in self.balance or not self.balance[s]['equity']:
                self.symbols.remove(s)

        self.longs = {s for s in self.symbols if self.settings[s]['long']}
        self.shrts = {s for s in self.symbols if self.settings[s]['shrt']}
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
        self.stream_tick_ts = 0

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
        if t is not None:
            print_([f'transfered {amount} {coin} to {symbol} isolated wallet'])
            self.balance[symbol][coin]['equity'] += amount
            if self.s2c[symbol] == coin:
                self.balance[symbol]['equity'] += amount / self.last_price[symbol]
            else:
                self.balance[symbol]['equity'] += amount

        return t

    async def transfer_from_isolated(self, symbol: str, coin: str, amount: float):
        # margin isolated transfer
        try:
            t = await self.cc.sapi_post_margin_isolated_transfer(
                params={'transTo': "SPOT",
                        'amount': amount,
                        'transFrom': "ISOLATED_MARGIN",
                        'asset': coin,
                        'symbol': symbol.replace('/', '')}
            )
        except:
            t = None
        if t is not None:
            print_([f'transfered {amount} {coin} from {symbol} isolated wallet'])
            self.balance[symbol][coin]['equity'] -= amount
            if self.s2c[symbol] == coin:
                self.balance[symbol]['equity'] -= amount / self.last_price[symbol]
            else:
                self.balance[symbol]['equity'] -= amount
        return t

    async def create_bid(self, symbol: str, amount: float, price: float) -> None:
        if self.is_executing('create_bid', symbol):
            return
        coin, quot = self.symbol_split[symbol]
        side_effect = 'MARGIN_BUY' if self.balance[symbol][quot]['free'] < amount * price \
            else 'NO_SIDE_EFFECT'
        bid = await tw(self.cc.sapi_post_margin_order, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE',
            'side': 'BUY',
            'type': 'LIMIT',
            'quantity': amount,
            'price': price,
            'timeInForce': 'GTC',
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

    async def create_ask(self, symbol: str, amount: float, price: float) -> None:
        if self.is_executing('create_ask', symbol):
            return
        coin, quot = self.symbol_split[symbol]
        side_effect = 'MARGIN_BUY' if self.balance[symbol][coin]['free'] < amount else \
            'NO_SIDE_EFFECT'
        ask = await tw(self.cc.sapi_post_margin_order, kwargs={'params': {
            'symbol': symbol.replace('/', ''),
            'isIsolated': 'TRUE',
            'side': 'SELL',
            'type': 'LIMIT',
            'quantity': amount,
            'price': price,
            'timeInForce': 'GTC',
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

    async def cancel_order(self, symbol: str, id_: int):
        if self.is_executing('cancel_order', symbol):
            return
        if int(id_) not in {int(o['order_id']) for o in self.open_orders[symbol]}:
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
                    asyncio.create_task(tw(self.update_balance, args=(symbol,)))
                    asyncio.create_task(tw(self.update_open_orders, args=(symbol,)))
                    asyncio.create_task(tw(self.update_my_trades, args=(symbol,)))
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
        self.balance[symbol][coin]['debt'] -= amount
        self.balance[symbol][coin]['onhand'] -= amount
        self.balance[symbol][coin]['free'] -= amount
        if coin == self.s2c[symbol]:
            cost = amount * self.last_price[symbol]
            self.balance[symbol]['debt'] -= cost
            self.balance[symbol]['total_account_debt'] -= cost
            self.balance[symbol]['total_account_onhand'] -= cost
        else:
            self.balance[symbol]['debt'] -= amount
            self.balance[symbol]['total_account_debt'] -= amount
            self.balance[symbol]['total_account_onhand'] -= amount
        self.timestamps['released']['repay'][sc] = time()
        result = {**{'symbol': symbol, 'coin': coin, 'amount': amount}, **repaid}
        if result is not None:
            print_(['   repaid',
                    [result[k] for k in ['symbol', 'coin', 'amount']]])
        return result

    async def update_borrowable(self, symbol: str, coin: str = None):
        if coin is None:
            await self.update_borrowable(symbol, self.s2c[symbol])
            await self.update_borrowable(symbol, self.s2q[symbol])
            return
        sc = symbol + coin
        _, quot = self.symbol_split[symbol]
        if self.is_executing('update_borrowable', sc):
            return
        print_(['updating borrowable', symbol, coin])
        try:
            fetched = await tw(self.get_borrowable, args=(symbol, coin))
            borrowable_ito_quot = max(0.0, (self.balance[symbol]['equity'] *
                                            (self.settings[symbol]['max_leverage'] - 1) -
                                             self.balance[symbol]['debt']))
            self.balance[symbol][coin]['borrowable'] = min(
                fetched['amount'] * 0.99,
                self.convert_amount(borrowable_ito_quot, quot, coin)
            )
        except Exception as e:
            track = traceback.format_exc()
            print(symbol, coin, e)
            print(track)
            self.balance[symbol][coin]['borrowable'] = 0.0
        self.timestamps['released']['update_borrowable'][sc] = time()

    async def update_balance(self, symbol: str = None):
        if symbol is None:
            if self.is_executing('update_balance', 'all'):
                return
            print_(['updating all balances'])
            now = time()
            for s in self.symbols:
                self.timestamps['locked']['update_balance'][s] = now
        else:
            if self.is_executing('update_balance', symbol):
                return
            else:
                print_(['updating balance', symbol])
        balances = await tw(self.get_balance, args=(symbol,))
        await asyncio.create_task(tw(self.dump_balance_log))
        await asyncio.create_task(tw(self.distribute_btc))
        for s in balances:
            coin, quot = self.symbol_split[s]
            if s not in self.symbols:
                continue
            try:
                if self.balance[s][coin]['equity'] != balances[s][coin]['equity'] or \
                        self.balance[s][quot]['equity'] != balances[s][quot]['equity']:
                    await asyncio.gather(*[tw(self.update_open_orders, args=(s,)),
                                           tw(self.update_my_trades, args=(s,)),
                                           tw(self.update_borrowable, args=(s, coin)),
                                           tw(self.update_borrowable, args=(s, quot))])
            except Exception as e:
                track = traceback.format_exc()
                #print('error updating balance', s, e, track)

            self.balance[s] = balances[s]
            self.timestamps['released']['update_balance'][s] = time()
        if symbol is None:
            self.timestamps['released']['update_balance']['all'] = time()

    async def get_balance(self, symbol: str = None):
        balances = {}
        if symbol is None:
            fetched = await tw(self.cc.sapi_get_margin_isolated_account)
            total_account_equity = float(fetched['totalNetAssetOfBtc'])
            total_account_debt = float(fetched['totalLiabilityOfBtc'])
            total_account_onhand = float(fetched['totalAssetOfBtc'])
        else:
            fetched = await tw(self.cc.sapi_get_margin_isolated_account,
                               kwargs={'params': {'symbols': self.dash_to_nodash[symbol]}})
            total_account_equity = None
            total_account_debt = None
            total_account_onhand = None

        for e in fetched['assets']:
            balance = {}
            s = self.nodash_to_dash[e['symbol']]
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
            balance[coin]['transferable'] = \
                max(0.0, min(balance[coin]['onhand'] - balance[coin]['debt'],
                             balance[coin]['free']))
            balance[quot]['transferable'] = \
                max(0.0, min(balance[quot]['onhand'] - balance[quot]['debt'],
                             balance[quot]['free']))
            if total_account_equity:
                balance['total_account_equity'] = total_account_equity
                balance['total_account_debt'] = total_account_debt
                balance['total_account_onhand'] = total_account_onhand
            elif 'total_account_equity' not in self.balance[s]:
                balance['total_account_equity'] = 1e-10
                balance['total_account_debt'] = 1e-10
                balance['total_account_onhand'] = 1e-10
            else:
                balance['total_account_equity'] = self.balance[s]['total_account_equity']
                balance['total_account_debt'] = self.balance[s]['total_account_debt']
                balance['total_account_onhand'] = self.balance[s]['total_account_onhand']
            try:
                balance['debt'] = (self.convert_amount(balance[coin]['debt'], coin, quot) +
                                   balance[quot]['debt'])
                balance['onhand'] = self.convert_amount(balance[coin]['onhand'], coin, quot) + \
                    balance[quot]['onhand']
                balance['entry_cost'] = max([
                    (self.settings[s]['account_equity_pct_per_entry'] *
                     balance['total_account_equity']),
                    self.min_trade_costs[s],
                    10**-self.amount_precisions[s] * self.last_price[s]
                ])
            except Exception as e:
                # track = traceback.format_exc()
                # print('error getting balance', s, e, track)
                pass
            balances[s] = balance
        return balances

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

    async def get_transferable(self, symbol: str, coin: str):
        borrowable = await tw(self.cc.sapi_get_margin_maxtransferable, kwargs={'params': {
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
            if self.my_trades[symbol]:
                from_id = self.my_trades[symbol][-1]['id'] + 1
            elif symbol in self.last_my_trades_id:
                from_id = self.last_my_trades_id[symbol] + 1
            else:
                from_id = 0
            fetched = await tw(self.fetch_my_trades,
                               args=(symbol,),
                               kwargs={'from_id': from_id})
            seen = set()
            mts = []
            for mt in sorted(self.my_trades[symbol] + fetched, key=lambda x: x['id']):
                self.last_my_trades_id[symbol] = mt['id']
                if mt['id'] in seen:
                    continue
                mts.append(mt)
                seen.add(mt['id'])
        else:
            mts = await tw(self.fetch_my_trades, args=(symbol,))
            self.last_my_trades_id[symbol] = mts[-1]['id'] if mts else 0
        mts = conglomerate_my_trades(mts)
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
                              self.balance[symbol]['entry_cost'],
                              entry_exit_amount_threshold)

        self.my_trades_analysis[symbol] = analysis
        self.sum_past_hour_long_entry_vol = \
            sum([self.my_trades_analysis[s]['past_hour_long_entry_vol']
                 for s in self.symbols if s in self.my_trades_analysis])
        self.sum_past_hour_shrt_entry_vol = \
            sum([self.my_trades_analysis[s]['past_hour_shrt_entry_vol']
                 for s in self.symbols if s in self.my_trades_analysis])
        self.my_trades[symbol] = my_trades
        self.timestamps['released']['update_my_trades'][symbol] = time()

    def on_update(self, s: str):
        now_millis = self.cc.milliseconds()
        if self.order_book[s]['bids'][-1]['price'] != self.prev_order_book[s]['bids'][-1]['price']:
            # highest bid changed
            if self.order_book[s]['bids'][-1]['price'] < \
                    self.prev_order_book[s]['bids'][-1]['price']:
                # highest bid became lower
                if self.my_bids[s]:
                    if self.order_book[s]['bids'][-1]['price'] < self.my_bids[s][-1]['price']:
                        print_(['bid taken', s])
                        self.my_trades_analysis['shrt_amount'] = 0.0
                        self.my_trades_analysis['last_long_entry_ts'] = now_millis
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        asyncio.create_task(tw(self.update_open_orders, args=(s,)))
                        asyncio.create_task(tw(self.update_my_trades, args=(s,)))
                    elif self.order_book[s]['bids'][-1]['price'] == self.my_bids[s][-1]['price']:
                        print_(['bid maybe taken', s])
                        self.my_trades_analysis['shrt_amount'] = 0.0
                        self.my_trades_analysis['last_long_entry_ts'] = now_millis
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        #asyncio.create_task(tw(self.update_my_trades, args=(s,)))
                        #asyncio.create_task(tw(self.update_open_orders, args=(s,)))
        if self.order_book[s]['asks'][0]['price'] != self.prev_order_book[s]['asks'][0]['price']:
            # lowest ask changed
            if self.order_book[s]['asks'][0]['price'] > \
                    self.prev_order_book[s]['asks'][0]['price']:
                # lowest ask became higher
                if self.my_asks[s]:
                    if self.order_book[s]['asks'][0]['price'] > self.my_asks[s][0]['price']:
                        print_(['ask taken', s])
                        self.my_trades_analysis['long_amount'] = 0.0
                        self.my_trades_analysis['last_shrt_entry_ts'] = now_millis
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        asyncio.create_task(tw(self.update_open_orders, args=(s,)))
                        asyncio.create_task(tw(self.update_my_trades, args=(s,)))
                    elif self.order_book[s]['asks'][0]['price'] == self.my_asks[s][0]['price']:
                        print_(['ask maybe taken', s])
                        self.my_trades_analysis['long_amount'] = 0.0
                        self.my_trades_analysis['last_shrt_entry_ts'] = now_millis
                        asyncio.create_task(tw(self.update_balance, args=(s,)))
                        #asyncio.create_task(tw(self.update_open_orders, args=(s,)))
                        #asyncio.create_task(tw(self.update_my_trades, args=(s,)))


        now = time()
        if now - self.timestamps['released']['update_balance']['all'] > \
                FORCE_UPDATE_INTERVAL_SECONDS:
            asyncio.create_task(tw(self.update_balance))
        for key0 in ['update_balance', 'update_open_orders', 'update_my_trades']:
            if now - self.timestamps['released'][key0][s] > \
                    FORCE_UPDATE_INTERVAL_SECONDS + np.random.choice(np.arange(-60, 60, 1)):
                asyncio.create_task(tw(getattr(self, key0), args=(s,)))
        for k in self.symbol_split[s]:
            if now - self.timestamps['released']['update_borrowable'][s + k] > \
                    FORCE_UPDATE_INTERVAL_SECONDS + np.random.choice(np.arange(-60, 60, 1)):
                asyncio.create_task(tw(self.update_borrowable, args=(s, k)))
        asyncio.create_task(tw(self.execute_to_exchange, args=(s,)))

    async def execute_to_exchange(self, s: str) -> None:
        now = time()
        if now - self.timestamps['released']['execute_to_exchange'][s] < 2.0:
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
        eligible_orders, repays = self.get_ideal_orders(s)
        orders_to_delete, orders_to_create = filter_orders(self.open_orders[s], eligible_orders)
        '''
        if orders_to_delete:
            print_(['debug', s, 'to delete', [[e[k] for k in ['side', 'amount', 'price']]
                                              for e in orders_to_delete]])
        if orders_to_create:
            print_(['debug', s, 'to create', [[e[k] for k in ['side', 'amount', 'price']]
                                              for e in orders_to_create]])
        '''
        if orders_to_delete:
            asyncio.create_task(tw(self.cancel_order, args=(s, orders_to_delete[0]['order_id'])))
        elif orders_to_create:
            o = orders_to_create[0]
            if o['side'] == 'buy':
                if len(self.my_bids[s]) < 3:
                    if 'liquidate' in o:
                        print_([f'{s} out of balance, too little {self.s2c[s]},'
                                'creating extra bid'])
                    asyncio.create_task(tw(self.create_bid, args=(s, o['amount'], o['price'])))
            elif o['side'] == 'sell':
                if len(self.my_asks[s]) < 3:
                    if 'liquidate' in o:
                        print_([f'{s} out of balance, too much {self.s2c[s]}, creating extra ask'])
                    asyncio.create_task(tw(self.create_ask, args=(s, o['amount'], o['price'])))
        elif repays:
            r = np.random.choice(repays)
            sc = r['symbol'] + r['coin']
            locked = self.timestamps['locked']['repay']
            if now - self.timestamps['released']['repay'][sc] > 59 * 60 and \
                    now - max(locked[key] for key in locked) > 5:
                print('DEBUG', s, r['coin'])
                print(self.balance[r['symbol']][r['coin']])
                asyncio.create_task(tw(self.repay, args=(r['symbol'], r['coin'], r['amount'])))

        '''
        for o in orders_to_delete[:2]:
            asyncio.create_task(tw(self.cancel_order,
                                   args=(s, o['order_id']),
                                   kwargs={'wait': False}))
            await asyncio.sleep(0.1)
        for o in orders_to_create[:1]:
            if o['side'] == 'buy':
                asyncio.create_task(tw(self.create_bid, args=(s, o['amount'], o['price'])))
                await asyncio.sleep(0.1)
            elif o['side'] == 'sell':
                asyncio.create_task(tw(self.create_ask, args=(s, o['amount'], o['price'])))
                await asyncio.sleep(0.1)
        '''
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

    def get_ideal_orders(self, s: str):

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
        shrt_sel_price = max([
            round_up(self.max_ema[s] * (1 + self.settings[s]['entry_spread']),
                     self.price_precisions[s]),
            other_bid_incr,
            (other_ask_decr if entry_cost / other_ask_decr < lowest_other_ask['amount']
             else lowest_other_ask['price'])
        ])
        long_buy_price = min([
            round_dn(self.min_ema[s] * (1 - self.settings[s]['entry_spread']),
                     self.price_precisions[s]),
            other_ask_decr,
            (other_bid_incr if entry_cost / other_bid_incr < highest_other_bid['amount']
             else highest_other_bid['price'])
        ])
        if self.settings[s]['phase_out']:
            if self.my_trades_analysis[s]['shrt_amount'] == 0.0:
                self.settings[s]['shrt'] = False
            if self.my_trades_analysis[s]['long_amount'] == 0.0:
                self.settings[s]['long'] = False
        if self.settings[s]['shrt']:
            shrt_amount_modifier = max(
                1.0,
                min(
                    self.settings[s]['min_exit_cost_multiplier'] / 2,
                    (self.last_price[s] /
                     (self.my_trades_analysis[s]['shrt_vwap'] if
                      self.my_trades_analysis[s]['shrt_vwap'] else self.last_price[s]))**exponent
                )
            ) if self.my_trades_analysis[s]['shrt_vwap'] > 0.0 else 1.0
            delay_hours = max(
                (min(self.my_trades_analysis[s]['last_shrt_entry_cost'], entry_cost) /
                 (self.settings[s]['account_equity_pct_per_hour'] *
                  self.balance[s]['total_account_equity'])),
                (self.sum_past_hour_shrt_entry_vol /
                 (self.global_settings['max_entry_acc_val_pct_per_hour'] *
                  self.balance[s]['total_account_equity']))
            )
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
            shrt_bag_duration_days = \
                (now_millis - self.my_trades_analysis[s]['shrt_start_ts']) / DAY_TO_MILLIS
            shrt_vwap_multiplier = 1 - max(
                self.settings[s]['min_markup_pct'],
                ((self.settings[s]['n_days_to_min_markup'] - shrt_bag_duration_days) /
                 self.settings[s]['n_days_to_min_markup']) * self.settings[s]['max_markup_pct']
            )
            shrt_buy_price = min([
                round_dn(min(self.min_ema[s],
                             self.my_trades_analysis[s]['shrt_vwap'] * shrt_vwap_multiplier),
                         self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr if shrt_buy_amount < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            if shrt_buy_amount * shrt_buy_price > min_exit_cost and \
                    shrt_buy_price > self.last_price[s] * 0.9:
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
            long_amount_modifier = max(
                1.0,
                min(
                    self.settings[s]['min_exit_cost_multiplier'] / 2,
                    (self.my_trades_analysis[s]['long_vwap'] /
                     self.last_price[s])**exponent
                )
            )
            delay_hours = max(
                (min(self.my_trades_analysis[s]['last_long_entry_cost'], entry_cost) /
                 (self.settings[s]['account_equity_pct_per_hour'] *
                  self.balance[s]['total_account_equity'])),
                (self.sum_past_hour_long_entry_vol /
                 (self.global_settings['max_entry_acc_val_pct_per_hour'] *
                  self.balance[s]['total_account_equity']))
            )
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
            long_bag_duration_days = \
                (now_millis - self.my_trades_analysis[s]['long_start_ts']) / DAY_TO_MILLIS
            long_vwap_multiplier = 1 + max(
                self.settings[s]['min_markup_pct'],
                ((self.settings[s]['n_days_to_min_markup'] - long_bag_duration_days) /
                 self.settings[s]['n_days_to_min_markup']) * self.settings[s]['max_markup_pct']
            )
            long_sel_price = max([
                round_up(max(self.max_ema[s],
                             self.my_trades_analysis[s]['long_vwap'] * long_vwap_multiplier),
                         self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr if long_sel_amount < lowest_other_ask['amount']
                 else lowest_other_ask['price'])
            ])
            if long_sel_amount * long_sel_price > min_exit_cost and \
                    long_sel_price < self.last_price[s] * 1.1:
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

        c, q = self.symbol_split[s]
        coin_available = max(0.0, self.balance[s][c]['onhand'] - self.balance[s][c]['debt'])
        quot_available = max(0.0, self.balance[s][q]['onhand'] - self.balance[s][q]['debt'])
        coin_credit_available = self.balance[s][c]['borrowable']
        quot_credit_available = self.balance[s][q]['borrowable']

        min_exit_cost = self.balance[s]['entry_cost'] * self.settings[s]['min_exit_cost_multiplier']
        eligible_orders = []
        o = self.ideal_orders[s]['long_buy']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if quot_available >= cost:
                eligible_orders.append(o)
                quot_available -= cost
            else:
                quot_plus_credit = quot_available + quot_credit_available
                if quot_available + quot_credit_available >= cost:
                    eligible_orders.append(o)
                    credit_spent = cost - quot_available
                    quot_credit_available -= credit_spent
                    coin_credit_available -= credit_spent / o['price']
                    quot_available = 0.0
                else:
                    new_amount = round_dn(quot_plus_credit / o['price'],
                                          self.amount_precisions[s])
                    if (new_cost := new_amount * o['price']) > self.min_trade_costs[s]:
                        o['amount'] = new_amount
                        eligible_orders.append(o)
                        quot_available = 0.0
                        quot_credit_available = 0.0
                        coin_credit_available = 0.0
        o = self.ideal_orders[s]['shrt_sel']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if coin_available >= o['amount']:
                eligible_orders.append(o)
                coin_available -= o['amount']
            else:
                coin_plus_credit = coin_available + coin_credit_available
                if coin_plus_credit >= o['amount']:
                    eligible_orders.append(o)
                    credit_spent = o['amount'] - coin_available
                    coin_credit_available -= credit_spent
                    quot_credit_available -= credit_spent * o['price']
                    coin_available = 0.0
                else:
                    new_amount = round_dn(coin_plus_credit, self.amount_precisions[s])
                    if new_amount * o['price'] > self.min_trade_costs[s]:
                        o['amount'] = new_amount
                        eligible_orders.append(o)

                        coin_available = 0.0
                        coin_credit_available = 0.0
                        quot_credit_available = 0.0
        o = self.ideal_orders[s]['long_sel']
        if (cost := o['price'] * o['amount']) > self.min_trade_costs[s]:
            if coin_available >= o['amount']:
                eligible_orders.append(o)
                coin_available -= o['amount']
            else:
                coin_plus_credit = coin_available + coin_credit_available
                if coin_plus_credit >= o['amount']:
                    eligible_orders.append(o)
                    credit_spent = o['amount'] - coin_available
                    coin_credit_available -= credit_spent
                    quot_credit_available -= credit_spent * o['price']
                    coin_available = 0.0
                else:
                    new_amount = round_dn(coin_plus_credit, self.amount_precisions[s])
                    if new_amount * o['price'] > min_exit_cost:
                        o['amount'] = new_amount
                        eligible_orders.append(o)
                        coin_available = 0.0
                        coin_credit_available = 0.0
                        quot_credit_available = 0.0
        o = self.ideal_orders[s]['shrt_buy']
        if (cost := o['price'] * o['amount']) > min_exit_cost:
            if quot_available >= cost:
                eligible_orders.append(o)
                quot_available -= cost
            else:
                quot_plus_credit = quot_available + quot_credit_available
                if quot_available + quot_credit_available >= cost:
                    eligible_orders.append(o)
                    credit_spent = cost - quot_available
                    quot_credit_available -= credit_spent
                    coin_credit_available -= credit_spent / o['price']
                    quot_available = 0.0
                else:
                    new_amount = round_dn(quot_plus_credit / o['price'],
                                          self.amount_precisions[s])
                    if (new_cost := new_amount * o['price']) > min_exit_cost:
                        o['amount'] = new_amount
                        eligible_orders.append(o)
                        quot_available = 0.0
                        quot_credit_available = 0.0
                        coin_credit_available = 0.0

        # extra order to liquidate coin equity

        if not self.settings[s]['long'] and not self.settings[s]['shrt']:
            extra_coin = self.balance[s][c]['equity']
        else:
            extra_coin = (self.balance[s][c]['equity'] +
                          self.my_trades_analysis[s]['shrt_amount'] -
                          self.my_trades_analysis[s]['long_amount'] +
                          self.ideal_orders[s]['long_buy']['amount'] -
                          self.ideal_orders[s]['shrt_sel']['amount'])

        if extra_coin > 0.0:
            liqui_price = max(self.ideal_orders[s]['long_sel']['price'], shrt_sel_price)
            liqui_amount = round_up(min(min_exit_cost / liqui_price, extra_coin),
                                    self.amount_precisions[s])
            if liqui_price / self.last_price[s] < 1.1 and \
                    extra_coin * liqui_price > min_exit_cost or \
                    (not self.settings[s]['long'] and
                     liqui_amount * liqui_price > self.min_trade_costs[s]) and \
                    coin_available + coin_credit_available > liqui_amount:
                #print_([f'{extra_coin:.4f} {c} leftover, creating extra ask'])
                eligible_orders.append({
                    'symbol': s,
                    'side': 'sell',
                    'amount': liqui_amount,
                    'price': liqui_price,
                    'liquidate': True,
                })
        else:
            liqui_price = min(self.ideal_orders[s]['shrt_buy']['price'], long_buy_price) if \
                self.ideal_orders[s]['shrt_buy']['price'] > 0.0 else long_buy_price
            liqui_amount = round_up(min(min_exit_cost / liqui_price, -extra_coin),
                                    self.amount_precisions[s])
            if self.last_price[s] / liqui_price < 1.1 and \
                    -extra_coin * liqui_price > min_exit_cost or \
                    (not self.settings[s]['shrt'] and
                     liqui_amount * liqui_price > self.min_trade_costs[s]) and \
                    quot_available + quot_credit_available > liqui_amount * liqui_price:
                #print_([f'{extra_coin:.4f} {c} leftover, creating extra bid'])
                eligible_orders.append({
                    'symbol': s,
                    'side': 'buy',
                    'amount': liqui_amount,
                    'price': liqui_price,
                    'liquidate': True,
                })

        # coin repay
        repays = []
        coin_repay_amount = max(0.0,
                                min([self.balance[s][c]['free'],
                                     self.balance[s][c]['debt'],
                                     (self.balance[s][c]['onhand'] -
                                      sum(e['amount']
                                          for e in eligible_orders
                                          if e['side'] == 'sell'))]))
        if coin_repay_amount > self.min_trade_costs[s] / self.last_price[s]:
            repays.append({'symbol': s, 'coin': c, 'amount': coin_repay_amount})
        quot_repay_amount = max(0.0,
                                min([self.balance[s][q]['free'],
                                     self.balance[s][q]['debt'],
                                     (self.balance[s][q]['onhand'] -
                                      sum(e['amount'] * e['price']
                                          for e in eligible_orders
                                          if e['side'] == 'buy'))]))
        if quot_repay_amount > self.min_trade_costs[s]:
            repays.append({'symbol': s, 'coin': q, 'amount': quot_repay_amount})

        return eligible_orders, repays

    async def distribute_btc(self, execute: bool = True):
        now = time()
        if now - self.timestamps['locked']['distribute_btc']['distribute_btc'] < 60 * 10:
            return
        print_(['distributing btc'])
        self.timestamps['locked']['distribute_btc']['distribute_btc'] = now
        amount = 0.0005
        quot_borrowable = sorted([(self.balance[s]['BTC']['borrowable'], s) for s in self.balance])
        s_to = [e[1] for e in quot_borrowable if e[1] in self.active_symbols][0]
        for e in quot_borrowable[::-1]:
            try:
                s_from = e[1]
                if s_from == s_to:
                    continue
                r = await self.transfer_from_isolated(s_from, 'BTC', amount)
                if type(r) == dict:
                    print(f'transfered {amount} btc from {s_from}', r)
                    break
                else:
                    print(f'failed to transfer {amount} btc from {s_from}')
            except:
                continue
        await asyncio.sleep(0.2)
        spot_bal = await self.cc.fetch_balance()
        spot_btc_free = spot_bal['BTC']['free']
        if spot_btc_free > 0.0:
            await self.transfer_to_isolated(s_to, 'BTC', spot_btc_free)
        await self.update_balance()
        await self.update_borrowable(s_from)
        await self.update_borrowable(s_to)

    async def dump_balance_log(self):
        interval = 60 * 60
        if self.is_executing('dump_balance_log', 'dump_balance_log'):
            return
        if time() - self.timestamps['released']['dump_balance_log']['dump_balance_log'] < interval:
            return
        print_(['dumping balance log'])
        filepath = make_get_filepath(
            f'logs/binance/{self.user}/isolated_margin_balances/balances.txt'
        )
        balance = await self.get_balance()
        for s in list(balance):
            if s in self.cc.markets and not balance[s]['equity']:
                del balance[s]
        with open(filepath, 'a') as f:
            line = json.dumps({**{'timestamp': self.cc.milliseconds()}, **balance}) + '\n'
            f.write(line)
        self.timestamps['released']['dump_balance_log']['dump_balance_log'] = time()


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


def analyze_my_trades(my_trades: [dict],
                      entry_cost: float,
                      entry_exit_amount_threshold: float) -> ([dict], dict):

    long_cost, long_amount = 0.0, 0.0
    shrt_cost, shrt_amount = 0.0, 0.0

    long_start_ts, shrt_start_ts = 0, 0
    last_long_entry_ts, last_shrt_entry_ts = 0, 0
    last_long_exit_ts, last_shrt_exit_ts = 0, 0
    last_long_entry_cost, last_shrt_entry_cost = 1e-10, 1e-10

    buy_cost, buy_amount = 0.0, 0.0
    sel_cost, sel_amount = 0.0, 0.0

    past_hour_long_entry_vol = 0
    past_hour_shrt_entry_vol = 0
    hour_ago_millis = (time() - 60 * 60) * 1000

    for mt in my_trades:
        if mt['side'] == 'buy':
            buy_cost += mt['cost']
            buy_amount += mt['amount']
            if mt['amount'] < entry_exit_amount_threshold:
                # long buy
                long_amount += mt['amount']
                long_cost += mt['cost']
                last_long_entry_ts = mt['timestamp']
                last_long_entry_cost = mt['cost']
                if mt['timestamp'] > hour_ago_millis:
                    past_hour_long_entry_vol += mt['cost']
            else:
                # shrt buy
                shrt_amount -= mt['amount']
                shrt_cost -= mt['cost']
                if shrt_amount <= entry_cost / mt['price'] or shrt_cost <= entry_cost:
                    shrt_start_ts = mt['timestamp']
                    shrt_amount = 0.0
                    shrt_cost = 0.0
        else:
            sel_cost += mt['cost']
            sel_amount += mt['amount']
            if mt['amount'] < entry_exit_amount_threshold:
                # shrt sel
                shrt_amount += mt['amount']
                shrt_cost += mt['cost']
                last_shrt_entry_ts = mt['timestamp']
                last_shrt_entry_cost = mt['cost']
                if mt['timestamp'] > hour_ago_millis:
                    past_hour_shrt_entry_vol += mt['cost']
            else:
                # long sel
                long_amount -= mt['amount']
                long_cost -= mt['cost']
                if long_amount <= entry_cost / mt['price'] or long_cost <= entry_cost:
                    long_start_ts = mt['timestamp']
                    long_amount = 0.0
                    long_cost = 0.0

    analysis = {'long_amount': long_amount,
                'long_cost': long_cost,
                'long_vwap': long_cost / long_amount if long_amount else 0.0,
                'shrt_amount': shrt_amount,
                'shrt_cost': shrt_cost,
                'shrt_vwap': shrt_cost / shrt_amount if shrt_amount else 0.0,
                'buy_vwap': buy_cost / buy_amount if buy_amount else 0.0,
                'sel_vwap': sel_cost / sel_amount if sel_amount else 0.0,
                'long_start_ts': long_start_ts,
                'shrt_start_ts': shrt_start_ts,
                'last_long_entry_ts': last_long_entry_ts,
                'last_shrt_entry_ts': last_shrt_entry_ts,
                'last_long_entry_cost': last_long_entry_cost,
                'last_shrt_entry_cost': last_shrt_entry_cost,
                'entry_exit_amount_threshold': entry_exit_amount_threshold,
                'entry_cost': entry_cost,
                'past_hour_long_entry_vol': past_hour_long_entry_vol,
                'past_hour_shrt_entry_vol': past_hour_shrt_entry_vol}

    start_ts = min(long_start_ts, shrt_start_ts) - 1000 * 60 * 60 * 24 * 30
    _, cropped_my_trades = partition_sorted(my_trades, lambda x: x['timestamp'] >= start_ts)
    return cropped_my_trades, analysis

def conglomerate_my_trades(mt: [dict]):
    ts_price_dict = defaultdict(list)
    for t in mt:
        ts_price_dict[(t['timestamp'], t['price'])].append(t)
    conglomerated = []
    for k in ts_price_dict:
        if len(ts_price_dict[k]) == 1:
            conglomerated.append(ts_price_dict[k][0])
        else:
            xs = ts_price_dict[k]
            conglomerated.append({'symbol': xs[0]['symbol'],
                                  'id': max(x['id'] for x in xs),
                                  'order_id': max(x['order_id'] for x in xs),
                                  'side': xs[0]['side'],
                                  'amount': sum(x['amount'] for x in xs),
                                  'price': xs[0]['price'],
                                  'cost': sum(x['cost'] for x in xs),
                                  'fee': sum(x['fee'] for x in xs),
                                  'fee_coin': xs[0]['fee_coin'],
                                  'timestamp': xs[0]['timestamp'],
                                  'datetime': xs[0]['datetime'],
                                  'is_maker': xs[0]['is_maker']})
    order_id_dict = defaultdict(list)
    for t in conglomerated:
        order_id_dict[t['order_id']].append(t)
    conglomerated = []
    for k in order_id_dict:
        if len(order_id_dict[k]) == 1:
            conglomerated.append(order_id_dict[k][0])
        else:
            xs = order_id_dict[k]
            conglomerated.append({'symbol': xs[0]['symbol'],
                                  'id': max(x['id'] for x in xs),
                                  'order_id': max(x['order_id'] for x in xs),
                                  'side': xs[0]['side'],
                                  'amount': sum(x['amount'] for x in xs),
                                  'price': xs[0]['price'],
                                  'cost': sum(x['cost'] for x in xs),
                                  'fee': sum(x['fee'] for x in xs),
                                  'fee_coin': xs[0]['fee_coin'],
                                  'timestamp': xs[0]['timestamp'],
                                  'datetime': xs[0]['datetime'],
                                  'is_maker': xs[0]['is_maker']})

    return sorted(conglomerated, key=lambda x: x['timestamp'])


def filter_orders(actual_orders: [dict],
                  ideal_orders: [dict],
                  keys: [str] = ['symbol', 'side', 'amount', 'price']) -> ([dict], [dict]):
    # returns (orders_to_delete, orders_to_create)

    if not actual_orders:
        return [], ideal_orders
    if not ideal_orders:
        return actual_orders, []
    actual_orders = actual_orders.copy()
    orders_to_create = []
    ideal_orders_cropped = [{k: o[k] for k in keys} for o in ideal_orders]
    actual_orders_cropped = [{k: o[k] for k in keys} for o in actual_orders]
    for ioc, io in zip(ideal_orders_cropped, ideal_orders):
        matches = [(aoc, ao) for aoc, ao in zip(actual_orders_cropped, actual_orders) if aoc == ioc]
        if matches:
            actual_orders.remove(matches[0][1])
            actual_orders_cropped.remove(matches[0][0])
        else:
            orders_to_create.append(io)
    return actual_orders, orders_to_create





async def main():
    user = sys.argv[1]
    print('user', user)
    settings = load_settings(user)
    bot = await create_bot(settings)
    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.cc.close()
        print('closed ccxt session')


if __name__ == '__main__':
    asyncio.run(main())
