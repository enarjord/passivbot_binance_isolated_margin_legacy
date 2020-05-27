from common_procedures import make_get_filepath, threaded, print_
from common_functions import ts_to_date, round_dn, round_up, remove_duplicates, partition_sorted
from commons import calc_other_orders, filter_orders
from threading import Lock
from time import time, sleep
from typing import Callable
from collections import OrderedDict
import pandas as pd
import os
import json

class Vwap:
    '''
    runs on margin wallet
    '''
    def __init__(self, commons, hyperparams: dict):
        self.cm = commons
        self.cc = commons.cc
        self.hyperparams = hyperparams
        self.hyperparams['profit_pct_plus'] = (1 + hyperparams['profit_pct'])
        self.hyperparams['profit_pct_minus'] = (1 - hyperparams['profit_pct'])
        self.hyperparams['account_equity_pct_per_period'] = \
            hyperparams['account_equity_pct_per_hour'] * hyperparams['hours_rolling_small_trade_window']
        self.hyperparams['millis_rolling_small_trade_window'] = \
            hyperparams['hours_rolling_small_trade_window'] * 60 * 60 * 1000
        self.hyperparams['max_memory_span_millis'] = hyperparams['max_memory_span_days'] * 1000 * 60 * 60 * 24
        self.user = hyperparams['user']
        self.symbols = hyperparams['symbols']
        self.symbols_set = set(self.symbols)
        self.coins = hyperparams['coins']
        self.quot = hyperparams['quot']
        self.all_coins_set = set(self.coins + [self.quot])
        self.symbol_split = {symbol: symbol.split('/') for symbol in self.symbols}
        self.do_shrt_sel = {symbol for symbol in self.symbols if 'BNB' not in symbol}
        self.do_shrt_buy = {symbol for symbol in self.symbols if 'BNB' not in symbol}
        self.do_long_buy = {symbol for symbol in self.symbols}
        self.do_long_sel = {symbol for symbol in self.symbols}
        self.do_borrow = {coin for coin in self.all_coins_set if coin != 'BNB'}
        self.nodash_to_dash_map = {symbol.replace('/', ''): symbol for symbol in self.symbols}
        self.balance = {}
        self.my_trades = {}
        self.my_trades_analyses = {s: {} for s in self.symbols}
        self.ideal_long_sel = {s: {'price': 0.0, 'amount': 0.0, 'side': 'sell'}
                               for s in self.symbols}
        self.ideal_shrt_buy = {s: {'price': 0.0, 'amount': 0.0, 'side': 'buy'}
                               for s in self.symbols}
        self.ideal_long_buy = {s: {'price': 0.0, 'amount': 0.0, 'side': 'buy'}
                               for s in self.symbols}
        self.ideal_shrt_sel = {s: {'price': 0.0, 'amount': 0.0, 'side': 'sell'}
                               for s in self.symbols}
        self.ideal_borrow = {c: 0.0 for c in self.coins}
        self.ideal_repay = {c: 0.0 for c in self.coins}
        self.my_bids = {symbol: [] for symbol in self.symbols}
        self.my_asks = {symbol: [] for symbol in self.symbols}
        self.borrowable = {coin: 0.0 for coin in self.coins}
        self.previous_ts_bid_taken = {s: 0.0 for s in self.symbols}
        self.previous_ts_ask_taken = {s: 0.0 for s in self.symbols}
        self.tradable_bnb = 0.0
        self.prev_sel_ts = {s: 0 for s in self.symbols}
        self.prev_loan_ts = {c: 0 for c in list(self.all_coins_set) + self.symbols + ['all']}
        self.borrow_history = {}
        self.repay_history = {}
        self.quot_locked_in_shrt_buys = 0.0

        self.time_keepers = {'update_balance': 0,
                             'update_open_orders': {symbol: 0 for symbol in self.symbols},
                             'update_my_trades': {symbol: 0 for symbol in self.symbols},
                             'update_borrow_history': {c: 0 for c in self.all_coins_set},
                             'update_repay_history': {c: 0 for c in self.all_coins_set},
                             'locks': {symbol: 0 for symbol in self.symbols},
                             'dump_balance_log': 0}
        self.updating_intervals = {'update_balance': 25,
                                   'update_open_orders': 60,
                                   'update_my_trades': 90,
                                   'dump_balance_log': 60 * 60}

        self.balance_log_filepath = make_get_filepath(
            f'logs/binance/{self.user}/balance_margin.txt')
        self.min_trade_costs = {symbol: self.cc.markets[symbol]['limits']['cost']['min']
                                  for symbol in self.cc.markets}
        self.price_precisions = {symbol: self.cc.markets[symbol]['precision']['price']
                                 for symbol in self.cc.markets}
        self.amount_precisions = {symbol: self.cc.markets[symbol]['precision']['amount']
                                  for symbol in self.cc.markets}
        self.locks = {symbol: Lock() for symbol in self.symbols}
        self.force_lock_release_timeout = 10
        self.running_updater = False
        self.last_stream_tick_ts = 0

    def init(self):
        self.update_balance()
        calls = []
        for i, symbol in enumerate(self.symbols):
            coin, quot = self.symbol_split[symbol]
            calls.append(threaded(self.update_my_trades)(symbol))
            sleep(0.1)
        [call.result() for call in calls]
        self.start_updater()

    def start_updater(self):
        if not self.running_updater:
            self.running_updater = True
            threaded(self.updater)()

    def stop_updater(self):
        self.running_updater = False

    def on_update(self, symbol: str, update: dict):
        try:
            if symbol not in self.symbols_set:
                return
            if update['bids'][-1]['price'] < self.cm.previous_updates[symbol]['bids'][-1]['price']:
                # highest bid became lower. was my bid taken
                if self.my_bids[symbol]:
                    if update['bids'][-1]['price'] <= self.my_bids[symbol][-1]['price']:
                        # maybe or yes
                        print_(['margin bid taken', symbol])
                        self.previous_ts_bid_taken[symbol] = time()
                        self.time_keepers['update_balance'] = 0
            if update['asks'][0]['price'] > self.cm.previous_updates[symbol]['asks'][0]['price']:
                # lowest ask became higher. was my ask taken
                if self.my_asks[symbol]:
                    if update['asks'][0]['price'] >= self.my_asks[symbol][0]['price']:
                        # maybe or yes
                        print_(['margin ask taken', symbol])
                        self.previous_ts_ask_taken[symbol] = time()
                        self.time_keepers['update_balance'] = 0
            if all([self.time_keepers[key][symbol] > 10
                    for key in ['update_open_orders', 'update_my_trades']]):
                self.try_wrapper(self.update_ideal_orders, (symbol,))
                self.try_wrapper(self.execute_to_exchange, (symbol,))
            self.last_stream_tick_ts = time()
        except(Exception) as e:
            print('\n\nERROR in on_update', e)

    def dump_balance_log(self):
        print_(['margin dumping balance'])
        with open(make_get_filepath(self.balance_log_filepath), 'a') as f:
            self.balance['timestamp'] = self.cc.milliseconds()
            line = json.dumps(self.balance) + '\n'
            f.write(line)
        self.time_keepers['dump_balance_log'] = time()

    def execute(self,
                symbol: str,
                fns: [Callable],
                args_list: [tuple] = [()],
                kwargs_list: [dict] = [{}],
                force_execution: bool = False):
        if not fns:
            return
        if force_execution or self.cm.can_execute(len(fns)):
            if self.locks[symbol].locked():
                if time() - self.time_keepers['locks'][symbol] > self.force_lock_release_timeout:
                    self.locks[symbol].release()
            else:
                threaded(self.lock_execute_release)(symbol, fns, args_list, kwargs_list)

    def updater(self):
        while self.running_updater:
            try:
                now = time()
                for key in self.updating_intervals:
                    if type(self.time_keepers[key]) == dict:
                        for symbol in self.time_keepers[key]:
                            if now - self.time_keepers[key][symbol] > self.updating_intervals[key]:
                                if 0 < self.time_keepers[key][symbol] < 10:
                                    self.time_keepers[key][symbol] += 1
                                else:
                                    self.time_keepers[key][symbol] = 1
                                    threaded(getattr(self, key))(symbol)
                            sleep(0.01)
                    else:
                        if now - self.time_keepers[key] > self.updating_intervals[key]:
                            if 0 < self.time_keepers[key] < 10:
                                self.time_keepers[key] += 1
                            else:
                                self.time_keepers[key] = 1
                                threaded(getattr(self, key))()
                        sleep(0.01)
                sleep(1)
            except(Exception) as e:
                print('ERROR with updater\n\n', e)
                raise Exception(e)

    def lock_execute_release(self,
                             symbol: str,
                             fns: [Callable],
                             args_list: [tuple],
                             kwargs_list: [dict]):
        try:
            self.locks[symbol].acquire()
            self.time_keepers['locks'][symbol] = time()
            self.cm.add_ts_to_lists(len(fns))
            future_results = []
            for i in range(len(fns)):
                args = args_list[i] if len(args_list) > i else ()
                kwargs = kwargs_list[i] if len(kwargs_list) > i else {}
                future_result = threaded(fns[i])(*args, **kwargs)
                future_results.append(future_result)
            for future_result in future_results:
                if future_result.exception():
                    if 'Unknown order sent' in future_result.exception().args[0]:
                        print_(['ERROR', 'margin', symbol,
                               'trying to cancel non-existing order; schedule open orders update'])
                        self.time_keepers['update_open_orders'][symbol] = 0
                    elif 'Account has insufficient balance' in future_result.exception().args[0]:
                        print_(['ERROR', 'margin', symbol, fns[i].__name__, args,
                                'insufficient funds; schedule balance update'])
                        self.time_keepers['update_balance'] = 0
                    else:
                        print_(['ERROR', 'margin', symbol, future_result.exception(), fns, args_list])
                else:
                    self.try_wrapper(self.update_after_execution, (future_result.result(),))
            self.locks[symbol].release()
        except(Exception) as e:
            print('\n\nERROR with lock_execute_release', e)

    def update_after_execution(self, update: dict):
        if update is None:
            return
        if 'tranId' in update:
            pass
        else:
            symbol = update['symbol']
            coin, quot = self.symbol_split[symbol]
            if update['status'] == 'canceled':
                if update['side'] == 'buy':
                    self.my_bids[symbol] = \
                        [e for e in self.my_bids[symbol] if e['id'] != update['id']]
                    self.balance[quot]['free'] += update['amount'] * update['price']
                else:
                    self.my_asks[symbol] = \
                        [e for e in self.my_asks[symbol] if e['id'] != update['id']]
                    self.balance[coin]['free'] += update['amount']
            elif update['type'] == 'limit':
                if update['side'] == 'buy':
                    self.my_bids[symbol] = sorted(self.my_bids[symbol] + [update],
                                                  key=lambda x: x['price'])
                    self.balance[quot]['free'] -= update['amount'] * update['price']
                else:
                    self.my_asks[symbol] = sorted(self.my_asks[symbol] + [update],
                                                  key=lambda x: x['price'])
                    self.balance[coin]['free'] -= update['amount']
        self.print_update(update)

    def print_update(self, update: dict):
        print_(['margin', {k: update[k] for k in ['coin', 'symbol', 'side', 'type', 'status',
                                                  'price', 'amount', 'tranId'] if k in update}])

    def update_balance(self):
        '''
        may be called threaded
        '''
        print_(['margin updating balance'])
        fetched = self.cc.sapi_get_margin_account()
        new_balance = {}
        onhand_sum_quot = 0.0
        debt_sum_quot = 0.0
        borrowable_quot = self.fetch_borrowable(self.quot)
        for e in fetched['userAssets']:
            c = e['asset']
            if c not in self.all_coins_set:
                continue
            new_balance[c] = {
                'free': float(e['free']),
                'used': float(e['locked']),
                'borrowed': float(e['borrowed']),
                'interest': float(e['interest']),
                'equity': float(e['netAsset']),
                'free': float(e['free']),
                'borrowable': (self.cm.convert_amount(borrowable_quot, self.quot, c)
                               if c in self.do_borrow else 0.0),
            }
            new_balance[c]['onhand'] = new_balance[c]['free'] + new_balance[c]['used']
            new_balance[c]['debt'] = new_balance[c]['interest'] + new_balance[c]['borrowed']
            onhand_sum_quot += self.cm.convert_amount(new_balance[c]['onhand'], c, self.quot)
            debt_sum_quot += self.cm.convert_amount(new_balance[c]['debt'], c, self.quot)
        for c in self.all_coins_set:
            if c not in new_balance:
                new_balance[c] = {key: 0.0 for key in ['free', 'used', 'borrowed', 'interest',
                                                       'onhand', 'equity', 'debt']}
                new_balance[c]['borrowable'] = \
                    (self.cm.convert_amount(borrowable_quot, self.quot, c)
                     if c in self.do_borrow else 0.0)
            new_balance[c]['account_onhand'] = self.cm.convert_amount(onhand_sum_quot, self.quot, c)
            new_balance[c]['account_debt'] = self.cm.convert_amount(debt_sum_quot, self.quot, c)
            new_balance[c]['account_equity'] = \
                new_balance[c]['account_onhand'] - new_balance[c]['account_debt']
            try:
                if self.balance[c]['onhand'] != new_balance[c]['onhand']:
                    symbol = f'{c}/{self.quot}'
                    if symbol in self.symbols:
                        self.time_keepers['update_open_orders'][symbol] = 0
                        self.time_keepers['update_my_trades'][symbol] = 0
            except(KeyError):
                pass
        self.tradable_bnb = max(0.0, new_balance['BNB']['onhand'] - self.hyperparams['bnb_buffer'])
        self.balance = new_balance
        self.time_keepers['update_balance'] = time()

    def update_open_orders(self, symbol: str):
        '''
        may be called threaded
        '''
        print_(['margin updating open orders', symbol])
        if symbol == 'all':
            symbols = set()
            self.my_asks = {s: [] for s in self.symbols}
            self.my_bids = {s: [] for s in self.symbols}
            for oo in sorted(self.fetch_margin_open_orders(),
                             key=lambda x: (x['symbol'], x['price'])):
                symbols.add(oo['symbol'])
                if oo['side'] == 'buy':
                    if oo['symbol'] not in self.my_bids:
                        self.my_bids[oo['symbol']] = []
                    self.my_bids[oo['symbol']].append(oo)
                else:
                    if oo['symbol'] not in self.my_asks:
                        self.my_asks[oo['symbol']] = []
                    self.my_asks[oo['symbol']].append(oo)
        else:
            symbols = [symbol]
            open_orders = self.fetch_margin_open_orders(symbol)
            self.my_bids[symbol], self.my_asks[symbol] = [], []
            for oo in sorted(open_orders, key=lambda x: x['price']):
                if oo['side'] == 'buy':
                    self.my_bids[symbol].append(oo)
                else:
                    self.my_asks[symbol].append(oo)
        now = time()
        for symbol in symbols:
            self.time_keepers['update_open_orders'][symbol] = now

    def init_my_trades(self, symbol: str) -> [dict]:
        no_dash = symbol.replace('/', '_')
        cache_filepath = make_get_filepath(f'cache/binance/{self.user}/my_trades/{no_dash}/')
        cached_history = []
        start_month = ts_to_date(self.cc.milliseconds() / 1000 - 60 * 60 * 24 * 90)[:7]
        filenames = [fp for fp in os.listdir(cache_filepath)
                     if fp.endswith('txt') and fp >= start_month]
        for filename in sorted(filenames):
            with open(cache_filepath + filename) as f:
                cached_history += [json.loads(line) for line in f.readlines()]
        cached_history = remove_duplicates(cached_history, key='id', sort=True)
        if cached_history == []:
            most_recent_id = 0
            limit = 1000
            from_id = 0
        else:
            most_recent_id = cached_history[-1]['id']
            limit = 100
            from_id = cached_history[-1]['id'] + 1
        fetched_history = []
        prev_my_trades = []
        while True:
            my_trades = self.fetch_margin_my_trades(symbol, limit, from_id)
            fetched_history += my_trades
            if len(my_trades) < limit or my_trades == prev_my_trades:
                break
            prev_my_trades = my_trades
            limit = 1000
            from_id = my_trades[-1]['id'] + 1
            sleep(1)
        written_history = self.write_cache(cache_filepath, fetched_history)
        my_trades = remove_duplicates(cached_history + written_history,
                                      key='id', sort=True)
        age_limit_millis = self.cc.milliseconds() - self.hyperparams['max_memory_span_millis']

        my_trades = [e for e in my_trades if e['timestamp'] > age_limit_millis]
        self.my_trades[symbol], self.my_trades_analyses[symbol] = \
            analyze_my_trades(my_trades)
        self.time_keepers['update_my_trades'][symbol] = time()

    def write_cache(self, filepath: str, items: [dict], condition: Callable = lambda x: True):
        written_items = []
        for d in remove_duplicates(items, key='id', sort=True):
            if condition(d):
                month = ts_to_date(d['timestamp'] / 1000)[:7]
                with open(f'{filepath}{month}.txt', 'a') as f:
                    f.write(json.dumps(d) + '\n')
                written_items.append(d)
        return written_items

    def update_my_trades(self, symbol: str):
        print_(['margin updating my trades', symbol])
        if symbol not in self.my_trades:
            self.init_my_trades(symbol)
        else:
            limit = 100
            new_my_trades = fetched_my_trades = self.fetch_margin_my_trades(
                symbol,
                limit=limit,
                from_id=self.my_trades[symbol][-1]['id'] + 1 if self.my_trades[symbol] else 0
            )
            prev_fetch = []
            while fetched_my_trades != prev_fetch and len(fetched_my_trades) == limit:
                limit = 1000
                prev_fetch = fetched_my_trades
                fetched_my_trades = self.fetch_margin_my_trades(
                    symbol, limit=limit, from_id=fetched_my_trades[-1]['id'] + 1)
                new_my_trades += fetched_my_trades
            if new_my_trades:
                no_dash = symbol.replace('/', '_')
                cache_filepath = f'cache/binance/{self.user}/my_trades/{no_dash}/'
                condition = lambda x: x['id'] > self.my_trades[symbol][-1]['id']
                written_my_trades = self.write_cache(cache_filepath, new_my_trades, condition)
                self.my_trades[symbol], self.my_trades_analyses[symbol] = \
                    analyze_my_trades(self.my_trades[symbol] + written_my_trades)

            age_limit_millis = self.cc.milliseconds() - self.hyperparams['max_memory_span_millis']
            my_trades = [e for e in self.my_trades[symbol] if e['timestamp'] > age_limit_millis]
            self.my_trades[symbol] = my_trades
            self.time_keepers['update_my_trades'][symbol] = time()

    def fetch_margin_my_trades(self, symbol: str, limit: int, from_id: int = -1):

        def format_mt(mt):
            formatted_mt = {'timestamp': mt['time'],
                            'datetime': ts_to_date(mt['time'] / 1000),
                            'symbol': self.nodash_to_dash_map[mt['symbol']],
                            'id': int(mt['id']),
                            'order_id': int(mt['orderId']),
                            'type': None,
                            'is_maker': mt['isMaker'],
                            'side': 'buy' if mt['isBuyer'] else 'sell',
                            'price': float(mt['price']),
                            'amount': float(mt['qty'])}
            formatted_mt['cost'] = formatted_mt['amount'] * formatted_mt['price']
            formatted_mt['fee_cost'] = float(mt['commission'])
            formatted_mt['fee_currency'] = mt['commissionAsset']
            return formatted_mt

        params = {'symbol': symbol.replace('/', ''), 'limit': limit}
        if from_id >= 0:
            params['fromId'] = from_id
        my_trades = self.cc.sapi_get_margin_mytrades(params=params)
        return sorted(map(format_mt, my_trades), key=lambda x: x['id'])

    def fetch_margin_open_orders(self, symbol: str = ''):

        def format_o(o):
            formatted_o = {'id': int(o['orderId']),
                           'timestamp': o['updateTime'],
                           'datetime': ts_to_date(o['updateTime'] / 1000),
                           'symbol': self.nodash_to_dash_map[o['symbol']],
                           'type': o['type'].lower(),
                           'side': o['side'].lower(),
                           'price': float(o['price']),
                           'amount': float(o['origQty']),
                           'cost': 0.0,
                           'average': None,
                           'filled': float(o['executedQty'])}
            formatted_o['remaining'] = formatted_o['amount'] - formatted_o['filled']
            formatted_o['status'] = 'open' if o['status'] == 'NEW' else o['status']
            return formatted_o

        if symbol == '':
            open_orders = self.cc.sapi_get_margin_openorders()
        else:
            open_orders = self.cc.sapi_get_margin_openorders(
                params={'symbol': symbol.replace('/', '')})
        return list(map(format_o, open_orders))

    def fetch_borrowable(self, coin: str) -> float:
        return float(self.cc.sapi_get_margin_maxborrowable(params={'asset': coin})['amount'])

    def create_margin_bid(self, symbol: str, amount: float, price: float):
        bid = self.cc.sapi_post_margin_order(
            params={'side': 'BUY', 'symbol': symbol.replace('/', ''), 'type': 'LIMIT',
                    'quantity': str(amount), 'price': price, 'timeInForce': 'GTC'})
        return {'id': int(bid['orderId']),
                'price': float(bid['price']),
                'amount': float(bid['origQty']),
                'symbol': symbol,
                'side': 'buy',
                'type': bid['type'].lower(),
                'status': 'open' if bid['status'] == 'NEW' else 'unknown'}

    def create_margin_ask(self, symbol: str, amount: float, price: float):
        ask = self.cc.sapi_post_margin_order(
            params={'side': 'SELL', 'symbol': symbol.replace('/', ''), 'type': 'LIMIT',
                    'quantity': str(amount), 'price': price, 'timeInForce': 'GTC'})
        return {'id': int(ask['orderId']),
                'price': float(ask['price']),
                'amount': float(ask['origQty']),
                'symbol': symbol,
                'side': 'sell',
                'type': ask['type'].lower(),
                'status': 'open' if ask['status'] == 'NEW' else 'unknown'}

    def cancel_margin_order(self, order_id: int, symbol: str):
        canceled = self.cc.sapi_delete_margin_order(
            params={'symbol': symbol.replace('/', ''), 'orderId': int(order_id)})
        return {'id': int(canceled['orderId']),
                'price': float(canceled['price']),
                'amount': float(canceled['origQty']),
                'symbol': symbol,
                'side': canceled['side'].lower(),
                'type': canceled['type'].lower(),
                'status': canceled['status'].lower()}

    def borrow(self, coin: str, amount: float):
        amount = round(amount, 8)
        if amount <= 0.0:
            return
        print_([f'borrowing {amount} {coin}'])
        borrowed = self.cc.sapi_post_margin_loan(params={'asset': coin, 'amount': amount})
        borrowed['coin'] = coin
        borrowed['amount'] = amount
        borrowed['side'] = 'borrow'
        borrowed['timestamp'] = self.cc.milliseconds()
        return borrowed

    def repay(self, coin: str, amount: float):
        amount = round(amount, 8)
        if amount <= 0.0:
            return
        print_([f'repaying {amount} {coin}'])
        repaid = self.cc.sapi_post_margin_repay(params={'asset': coin, 'amount': amount})
        repaid['coin'] = coin
        repaid['amount'] = amount
        repaid['side'] = 'repay'
        repaid['timestamp'] = self.cc.milliseconds()
        return repaid

    def update_ideal_orders(self, s: str):
        coin, quot = self.symbol_split[s]

        other_bids = calc_other_orders(self.my_bids[s], self.cm.order_book[s]['bids'])
        other_asks = calc_other_orders(self.my_asks[s], self.cm.order_book[s]['asks'])

        highest_other_bid = sorted(other_bids, key=lambda x: x['price'])[-1] if other_bids else \
            self.my_bids[s][-1]
        lowest_other_ask = sorted(other_asks, key=lambda x: x['price'])[0] if other_asks else \
            self.my_asks[s][0]

        other_bid_incr = round(highest_other_bid['price'] + 10**-self.price_precisions[s],
                               self.price_precisions[s])
        other_ask_decr = round(lowest_other_ask['price'] - 10**-self.price_precisions[s],
                               self.price_precisions[s])

        small_trade_cost_default = max(
            self.min_trade_costs[s],
            (self.balance[quot]['account_equity'] *
             self.hyperparams['account_equity_pct_per_trade'])
        )

        small_trade_cost = max(
            10**-self.amount_precisions[s] * self.cm.last_price[s],
            small_trade_cost_default
        )
        approx_small_trade_amount = round_up(small_trade_cost / self.cm.last_price[s],
                                             self.amount_precisions[s])
        min_big_trade_amount = approx_small_trade_amount * 6
        long_cost_vol, shrt_cost_vol = calc_rolling_cost_vol(
            self.my_trades[s],
            self.my_trades_analyses[s]['small_big_amount_threshold'],
            self.cc.milliseconds() - self.hyperparams['millis_rolling_small_trade_window']
        )

        all_shrt_buys = []
        for s_ in self.symbols:
            price_ = self.my_trades_analyses[s_]['true_shrt_vwap'] * \
                self.hyperparams['profit_pct_minus']
            c_, q_ = self.symbol_split[s_]
            amount_ = self.balance[c_]['borrowed']
            cost_ = amount_ * price_
            all_shrt_buys.append({'price': price_, 'amount': amount_, 'cost': cost_, 'symbol': s_})

        quot_locked_in_shrt_buys = sum([e['cost'] for e in all_shrt_buys])
        quot_locked_in_long_buys = \
            sum([self.ideal_long_buy[s_]['amount'] * self.ideal_long_buy[s_]['price']
                 for s_ in self.symbols])
        self.quot_locked_in_shrt_buys = quot_locked_in_shrt_buys

        # small orders #

        # long_buy #
        if s in self.do_long_buy:
            long_buy_cost = min([small_trade_cost,
                                 self.balance[quot]['onhand'] / len(self.symbols),
                                 (self.balance[quot]['account_equity'] *
                                  self.hyperparams['account_equity_pct_per_period'] -
                                  long_cost_vol)])
            long_buy_cost = long_buy_cost if long_buy_cost > self.min_trade_costs[s] else 0.0
            long_buy_price = min([
                round_dn(self.cm.min_ema[s], self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr
                 if long_buy_cost / other_bid_incr < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            self.ideal_long_buy[s] = {
                'side': 'buy',
                'amount': round_up(long_buy_cost / long_buy_price, self.amount_precisions[s]),
                'price': long_buy_price
            }
        else:
            self.ideal_long_buy[s] = {'side': 'buy', 'amount': 0.0, 'price': 0.0}
        ############

        # shrt_sel #
        if s in self.do_shrt_sel:
            shrt_sel_cost = min([
                small_trade_cost,
                self.tradable_bnb if coin == 'BNB' else self.balance[coin]['onhand'],
                (self.balance[quot]['account_equity'] *
                 self.hyperparams['account_equity_pct_per_period'] -
                 shrt_cost_vol)
            ])
            shrt_sel_cost = shrt_sel_cost if shrt_sel_cost > self.min_trade_costs[s] else 0.0
            shrt_sel_price = max([
                round_up(self.cm.max_ema[s], self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr
                 if shrt_sel_cost / other_ask_decr < lowest_other_ask['amount']
                 else lowest_other_ask['price'])
            ])
            shrt_sel_amount = min(
                round_dn(self.balance[coin]['onhand'] + self.ideal_borrow[coin],
                         self.amount_precisions[s]),
                round_up(shrt_sel_cost / shrt_sel_price, self.amount_precisions[s])
            )
            if shrt_sel_amount > self.min_trade_costs[s] / shrt_sel_price:
                self.ideal_shrt_sel[s] = {
                    'side': 'sell',
                    'amount': shrt_sel_amount,
                    'price': shrt_sel_price
                }
            else:
                self.ideal_shrt_sel[s] = {'side': 'sell', 'amount': 0.0, 'price': 0.0}
        else:
            self.ideal_shrt_sel[s] = {'side': 'sell', 'amount': 0.0, 'price': 0.0}
        ###########

        # debt adjustments #

        ideal_quot_onhand = quot_locked_in_shrt_buys + quot_locked_in_long_buys
        self.ideal_borrow[quot] = max(0.0, min(ideal_quot_onhand - self.balance[quot]['onhand'],
                                               self.balance[quot]['borrowable']))
        ideal_repay_quot = max(0.0, min([(self.balance[quot]['borrowed'] +
                                          self.balance[quot]['interest']),
                                         self.balance[quot]['free'],
                                         self.balance[quot]['onhand'] - ideal_quot_onhand]))
        self.ideal_repay[quot] = ideal_repay_quot \
            if ideal_repay_quot > quot_locked_in_long_buys else 0.0

        #------------------#

        ideal_coin_debt = self.my_trades_analyses[s]['true_shrt_amount'] + \
            self.ideal_shrt_sel[s]['amount']

        self.ideal_borrow[coin] = max(0.0, min(self.balance[coin]['borrowable'],
                                               ideal_coin_debt - self.balance[coin]['onhand']))
        ideal_repay_coin = min([self.balance[coin]['free'],
                                self.balance[coin]['debt'],
                                self.balance[coin]['onhand'] - ideal_coin_debt])
        self.ideal_repay[coin] = ideal_repay_coin \
            if ideal_repay_coin > approx_small_trade_amount * 2 else 0.0

        ####################

        # big orders #

        # long_sel #
        if s in self.do_long_sel:
            long_sel_amount = (
                (self.tradable_bnb if coin == 'BNB' else self.balance[coin]['onhand']) -
                self.ideal_shrt_sel[s]['amount'] -
                self.ideal_repay[coin] +
                self.ideal_borrow[coin]
            )
            long_sel_amount = max(0.0, round_dn(long_sel_amount, self.amount_precisions[s]))
            if long_sel_amount > min_big_trade_amount:
                long_sel_price = max([
                    round_up(self.cm.max_ema[s], self.price_precisions[s]),
                    other_bid_incr,
                    (other_ask_decr
                     if long_sel_amount < lowest_other_ask['amount']
                     else lowest_other_ask['price']),
                    round_up((self.my_trades_analyses[s]['true_long_vwap'] *
                              self.hyperparams['profit_pct_plus']),
                             self.price_precisions[s])
                ])
            else:
                long_sel_price = 0.0
            self.ideal_long_sel[s] = {'side': 'sell',
                                      'amount': long_sel_amount,
                                      'price': long_sel_price}
        else:
            self.ideal_long_sel[s] = {'side': 'sell', 'amount': 0.0, 'price': 0.0}
        ############

        # shrt_buy #
        if s in self.do_shrt_buy:
            shrt_buy_amount = (self.balance[coin]['borrowed'] +
                               self.ideal_borrow[coin] -
                               self.ideal_repay[coin] -
                               self.ideal_shrt_sel[s]['amount'])
            shrt_buy_amount = max(0.0, round_dn(shrt_buy_amount))
            shrt_buy_price = min([
                round_dn(self.cm.min_ema[s], self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr
                 if shrt_buy_amount < highest_other_bid['amount']
                 else highest_other_bid['price']),
                round_dn((self.my_trades_analyses[s]['true_shrt_vwap'] *
                          self.hyperparams['profit_pct_minus']),
                         self.price_precisions[s]),
            ])
            if shrt_buy_amount > min_big_trade_amount:
                if self.balance[quot]['onhand'] + self.ideal_borrow[quot] < ideal_quot_onhand:
                    # means we are max leveraged
                    shrt_buys_sorted_by_price_diff = sorted(
                        [e for e in all_shrt_buys if e['cost'] > small_trade_cost_default * 6],
                        key=lambda x: self.cm.last_price[x['symbol']] / x['price']
                    )
                    tmp_sum = quot_locked_in_long_buys
                    eligible_shrt_buys = []
                    for sb in shrt_buys_sorted_by_price_diff:
                        tmp_sum += sb['cost']
                        if tmp_sum >= self.balance[quot]['onhand']:
                            break
                        eligible_shrt_buys.append(sb)
                    if s not in set(map(lambda x: x['symbol'], eligible_shrt_buys)):
                        shrt_buy_price = 0.0
            else:
                shrt_buy_price = 0.0
            self.ideal_shrt_buy[s] = {'side': 'buy',
                                      'amount': shrt_buy_amount,
                                      'price': shrt_buy_price}
        else:
            self.ideal_shrt_buy[s] = {'side': 'buy', 'amount': 0.0, 'price': 0.0}

    def execute_to_exchange(self, s: str):
        # s == symbol

        coin, quot = self.symbol_split[s]
        for ts in [self.time_keepers['update_balance'],
                   self.time_keepers['update_my_trades'][s],
                   self.time_keepers['update_open_orders'][s]]:
            if ts < 11:
                return

        now = time()
        for side in ['borrow', 'repay']:
            for coin_ in [quot, coin]:
                amount = getattr(self, f'ideal_{side}')[coin_]
                if amount > 0.0 and now - self.prev_loan_ts[s] > 10 and \
                        now - self.prev_loan_ts[coin_] > 10 and \
                        now - self.prev_loan_ts['all'] > 1:
                    self.try_wrapper(self.execute, (s, [getattr(self, side)], [(coin_, amount)]))
                    self.prev_loan_ts[s] = now
                    self.prev_loan_ts[coin_] = now
                    self.prev_loan_ts['all'] = now

        ideal_bids = [bid for bid in [self.ideal_long_buy[s], self.ideal_shrt_buy[s]]
                      if bid['amount'] > 0.0 and bid['price'] > 0.0]

        bid_deletions, bid_creations = filter_orders(self.my_bids[s], ideal_bids)

        fns = []
        args_list = []
        if bid_deletions:
            fns += [self.cancel_margin_order] * len(bid_deletions)
            args_list += [(deletion['id'], s) for deletion in bid_deletions]
        if bid_creations:
            fns += [self.create_margin_bid] * len(bid_creations)
            args_list += [(s, creation['amount'], creation['price']) for creation in bid_creations]
        self.try_wrapper(self.execute, (s, fns, args_list))

        ideal_asks = [ask for ask in [self.ideal_shrt_sel[s], self.ideal_long_sel[s]]
                      if ask['amount'] > 0.0 and ask['price'] > 0.0]

        ask_deletions, ask_creations = filter_orders(self.my_asks[s], ideal_asks)

        fns = []
        args_list = []
        if ask_deletions:
            fns += [self.cancel_margin_order] * len(ask_deletions)
            args_list += [(deletion['id'], s) for deletion in ask_deletions]
        if ask_creations:
            fns += [self.create_margin_ask] * len(ask_creations)
            args_list += [(s, creation['amount'], creation['price']) for creation in ask_creations]
        self.try_wrapper(self.execute, (s, fns, args_list))

    def try_wrapper(self, fn, args=(), kwargs={}, comment: str = ''):
        try:
            result = fn(*args, **kwargs)
            return result
        except(Exception) as e:
            print(f'ERROR with {fn.__name__} {args} {kwargs}', repr(e), comment)
            return None


    def update_borrow_history(self, coin: str):
        self.try_wrapper(self.update_loan_history, (coin, 'borrow'))

    def update_repay_history(self, coin: str):
        self.try_wrapper(self.update_loan_history, (coin, 'repay'))

    def update_loan_history(self, coin: str, side: str):
        assert side in ['borrow', 'repay']
        if coin == self.quot:
            return
        elif coin == 'BNB':
            getattr(self, f'{side}_history')[coin] = []
            return
        print_([f'margin update {side} history', coin])
        if coin not in getattr(self, f'{side}_history'):
            self.init_loan_history(coin, side)
        else:
            old_loans = getattr(self, f'{side}_history')[coin]
            fetched = new_loans = getattr(self, f'fetch_{side}_history')(coin)
            prev_fetch = []
            most_recent_id = old_loans[-1]['id'] if old_loans else 0
            current = 2
            while fetched and fetched != prev_fetch and fetched[0]['id'] > most_recent_id:
                prev_fetch = fetched
                fetched = getattr(self, f'fetch_{side}_history')(coin, current)
                new_loans = fetched + new_loans
                current += 1
            condition = lambda x: x['id'] > most_recent_id
            cache_filepath = f'cache/binance/{self.user}/{side}_history/{coin}/'
            written_loans = self.write_cache(cache_filepath, new_loans, condition)
            getattr(self, f'{side}_history')[coin] += written_loans
        self.time_keepers[f'update_{side}_history'][coin] = time()

    def init_loan_history(self, coin: str, side: str):
        assert side in ['borrow', 'repay']
        cache_filepath = make_get_filepath(f'cache/binance/{self.user}/{side}_history/{coin}/')
        cached_loan_history = []
        start_month = ts_to_date(self.cc.milliseconds() / 1000 - 60 * 60 * 24 * 90)[:7]
        filenames = [fname for fname in os.listdir(cache_filepath)
                     if fname.endswith('txt') and fname >= start_month]
        for filename in sorted(filenames):
            with open(cache_filepath + filename) as f:
                cached_loan_history += [json.loads(line) for line in f.readlines()]
        cached_loan_history = remove_duplicates(cached_loan_history, key='id', sort=True)
        if cached_loan_history:
            most_recent_id = cached_loan_history[-1]['id']
            size = 10
        else:
            most_recent_id = 0
            size = 100
        fetched_loan_history = []
        current = 1
        while True:
            loans = getattr(self, f'fetch_{side}_history')(coin, current, size)
            fetched_loan_history += loans
            if len(loans) < size or loans[0]['id'] <= most_recent_id:
                break
            current += 1
            size = 100
        fetched_loan_history = sorted(fetched_loan_history, key=lambda x: x['id'])
        condition = lambda x: x['id'] > most_recent_id
        written_loan_history = self.write_cache(cache_filepath, fetched_loan_history, condition)
        getattr(self, f'{side}_history')[coin] = remove_duplicates(
            cached_loan_history + written_loan_history, key='id', sort=True)
        self.time_keepers[f'update_{side}_history'][coin] = time()


    def fetch_borrow_history(self, coin: str, current: int = 1, size: int = 10):
        '''
        will fetch 10 most recent loans
        '''
        history = self.cc.sapi_get_margin_loan(params={'asset': coin,
                                                       'startTime': 0,
                                                       'current': current,
                                                       'size': size})
        return sorted([{'amount': float(e['principal']),
                        'timestamp': e['timestamp'],
                        'id': e['txId'],
                        'coin': e['asset'],
                        'status': e['status'],
                        'side': 'borrow'}
                       for e in history['rows'] if e['status'] == 'CONFIRMED'],
                      key=lambda x: x['id'])

    def fetch_repay_history(self, coin: str, current: int = 1, size: int = 10):
        '''
        will fetch 10 most recent repays
        '''
        history = self.cc.sapi_get_margin_repay(params={'asset': coin,
                                                        'startTime': 0,
                                                        'current': current,
                                                        'size': size})
        return sorted([{'amount': float(e['principal']),
                        'timestamp': e['timestamp'],
                        'id': e['txId'],
                        'coin': e['asset'],
                        'status': e['status'],
                        'side': 'repay'}
                       for e in history['rows'] if e['status'] == 'CONFIRMED'],
                      key=lambda x: x['id'])


####################################################################################################
####################################################################################################
####################################################################################################


def calc_small_big_threshold_amount(my_trades: [dict], cutoff: float = 0.83333, m: float = 2.1):
    if len(my_trades) < 10:
        return 9e9
    return sorted((amts := [e['amount'] for e in my_trades]))[:int(len(amts) * cutoff)][-1] * m


def calc_rolling_cost_vol(my_trades: [dict],
                          small_big_amount_threshold: float,
                          age_limit_millis: int) -> (float, float):
    long_cost, shrt_cost = 0.0, 0.0
    long_done, shrt_done = False, False
    for mt in my_trades[::-1]:
        if long_done and shrt_done:
            break
        if mt['side'] == 'buy':
            if mt['amount'] < small_big_amount_threshold:
                # long buy
                if not long_done:
                    if mt['timestamp'] <= age_limit_millis:
                        long_done = True
                    else:
                        long_cost += mt['cost']
            else:
                # shrt buy
                shrt_done = True
        else:
            if mt['amount'] < small_big_amount_threshold:
                # shrt sel
                if not shrt_done:
                    if mt['timestamp'] <= age_limit_millis:
                        shrt_done = True
                    else:
                        shrt_cost += mt['cost']
            else:
                # long sel
                long_done = True
    return long_cost, shrt_cost


def analyze_my_trades(my_trades: [dict]) -> ([dict], dict):

    small_big_amount_threshold = calc_small_big_threshold_amount(my_trades)

    long_cost, long_amount = 0.0, 0.0
    shrt_cost, shrt_amount = 0.0, 0.0

    long_start_ts, shrt_start_ts = 0, 0


    for mt in my_trades:
        if mt['side'] == 'buy':
            if mt['amount'] < small_big_amount_threshold:
                # long buy
                long_amount += mt['amount']
                long_cost += mt['cost']
            else:
                # shrt buy
                shrt_amount -= mt['amount']
                shrt_cost -= mt['cost']
                if shrt_amount < 0.0 or shrt_cost < 0.0:
                    shrt_start_ts = mt['timestamp']
                    shrt_amount = 0.0
                    shrt_cost = 0.0
        else:
            if mt['amount'] < small_big_amount_threshold:
                # shrt sel
                shrt_amount += mt['amount']
                shrt_cost += mt['cost']
            else:
                # long sel
                long_amount -= mt['amount']
                long_cost -= mt['cost']
                if long_amount < 0.0 or long_cost < 0.0:
                    long_start_ts = mt['timestamp']
                    long_amount = 0.0
                    long_cost = 0.0

    analysis = {'true_long_amount': long_amount,
                'true_long_cost': long_cost,
                'true_long_vwap': long_cost / long_amount if long_amount else 0.0,
                'true_shrt_amount': shrt_amount,
                'true_shrt_cost': shrt_cost,
                'true_shrt_vwap': shrt_cost / shrt_amount if shrt_amount else 0.0,
                'long_start_ts': long_start_ts,
                'shrt_start_ts': shrt_start_ts,
                'small_big_amount_threshold': small_big_amount_threshold}

    start_ts = min(long_start_ts, shrt_start_ts) - 1000 * 60 * 60 * 24 * 7
    _, cropped_my_trades = partition_sorted(my_trades, lambda x: x['timestamp'] >= start_ts)
    return cropped_my_trades, analysis


