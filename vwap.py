from common_procedures import make_get_filepath, threaded, print_
from common_functions import ts_to_date, round_dn, round_up, remove_duplicates, partition_sorted, \
    flatten
from commons import calc_other_orders, filter_orders
from threading import Lock
from time import time, sleep
from typing import Callable
import os
import json
import sys

class Vwap:
    '''
    runs on margin wallet
    '''
    def __init__(self, commons, settings: dict):
        self.cm = commons
        self.cc = commons.cc
        self.settings = settings
        self.settings['profit_pct_plus'] = (1 + settings['profit_pct'])
        self.settings['profit_pct_minus'] = (1 - settings['profit_pct'])
        self.settings['account_equity_pct_per_period'] = \
            settings['account_equity_pct_per_hour'] * \
            settings['hours_rolling_small_trade_window']
        self.settings['millis_rolling_small_trade_window'] = \
            settings['hours_rolling_small_trade_window'] * 60 * 60 * 1000
        self.settings['max_memory_span_millis'] = \
            settings['max_memory_span_days'] * 1000 * 60 * 60 * 24
        self.settings['entry_spread_plus'] = 1 + settings['entry_spread'] / 2
        self.settings['entry_spread_minus'] = 1 - settings['entry_spread'] / 2
        self.user = settings['user']
        self.symbols = settings['symbols']
        self.symbols_set = set(self.symbols)
        self.coins = settings['coins']
        self.quot = settings['quot']
        self.all_coins_set = set(list(self.coins) + [self.quot])
        self.symbol_split = {symbol: symbol.split('/') for symbol in self.symbols}
        self.do_shrt_sel = {symbol for symbol in self.symbols
                            if self.symbol_split[symbol][0] in settings['coins_shrt']}
        self.do_long_buy = {symbol for symbol in self.symbols
                            if self.symbol_split[symbol][0] in settings['coins_long']}
        self.do_shrt_buy = {symbol for symbol in self.symbols}
        self.do_long_sel = {symbol for symbol in self.symbols}
        self.do_borrow = {coin for coin in self.all_coins_set
                          if coin not in settings['do_not_borrow']}
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
        self.ideal_borrow = {c: 0.0 for c in self.all_coins_set}
        self.ideal_repay = {c: 0.0 for c in self.all_coins_set}
        self.my_bids = {symbol: [] for symbol in self.symbols}
        self.my_asks = {symbol: [] for symbol in self.symbols}
        self.open_orders = {}
        self.tradable_bnb = 0.0
        self.borrow_history = {}
        self.repay_history = {}
        self.d = {k: {} for k in set(self.symbols + list(self.all_coins_set))}
        self.eligible_entries = []
        self.eligible_exits = []
        self.liquidation_orders = {}
        self.prev_execution_ts = 0
        self.prev_loan_ts = {c_: 0 for c_ in sorted(self.all_coins_set) + ['all']}
        self.prev_repay_ts = {c_: 0 for c_ in self.all_coins_set}

        self.time_keepers = {'update_balance': 0,
                             'update_open_orders': {symbol: 0 for symbol in self.symbols},
                             'update_my_trades': {symbol: 0 for symbol in self.symbols},
                             'update_borrow_history': {c: 0 for c in self.all_coins_set},
                             'update_repay_history': {c: 0 for c in self.all_coins_set},
                             'future_handling_lock': 0,
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
        self.future_handling_lock = Lock()
        self.force_lock_release_timeout = 10
        self.running_updater = False
        self.last_stream_tick_ts = 0
        self.counter = 0

    def init(self):
        self.update_balance()
        calls = []
        for i, symbol in enumerate(self.symbols):
            coin, quot = self.symbol_split[symbol]
            calls.append(threaded(self.update_my_trades)(symbol))
            sleep(0.1)
        recent_repays_future = {}
        print()
        for coin in self.all_coins_set:
            sys.stdout.write(f'\rfetching {coin} last repay ts      ')
            sys.stdout.flush()
            recent_repays_future[coin] = threaded(self.fetch_repay_history)(coin)
            sleep(0.1)
        print()
        [call.result() for call in calls]
        recent_repays = {c: recent_repays_future[c].result() for c in recent_repays_future}
        self.prev_repay_ts = {c: (recent_repays[c][-1]['timestamp'] if recent_repays[c] else 0)
                              for c in self.all_coins_set}
        for s_ in self.symbols:
            self.set_ideal_orders(s_)
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
                        self.time_keepers['update_balance'] = 0
            if update['asks'][0]['price'] > self.cm.previous_updates[symbol]['asks'][0]['price']:
                # lowest ask became higher. was my ask taken
                if self.my_asks[symbol]:
                    if update['asks'][0]['price'] >= self.my_asks[symbol][0]['price']:
                        # maybe or yes
                        print_(['margin ask taken', symbol])
                        self.time_keepers['update_balance'] = 0
            self.try_wrapper(self.set_ideal_orders, (symbol,))
            self.try_wrapper(self.execute_to_exchange)
            self.last_stream_tick_ts = time()
        except(Exception) as e:
            print('\n\nERROR in on_update', e)

    def dump_balance_log(self):
        print_(['margin dumping balance'])
        with open(make_get_filepath(self.balance_log_filepath), 'a') as f:
            line = json.dumps({**{'timestamp': self.cc.milliseconds()}, **self.balance}) + '\n'
            f.write(line)
        self.time_keepers['dump_balance_log'] = time()

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
                if (secs_since_tick := now - self.last_stream_tick_ts) > 30:
                    print_([f'warning, {secs_since_tick} seconds since last websocket tick',
                            'streamer probably not running'])
            except(Exception) as e:
                print('ERROR with updater\n\n', e)
                raise Exception(e)

    def update_after_execution(self, update: dict):
        if update is None:
            return
        if 'tranId' in update:
            pass
        else:
            symbol = update['symbol']
            coin, quot = self.symbol_split[symbol]
            if update['status'] == 'canceled':
                self.open_orders[symbol] = \
                    [o for o in self.open_orders[symbol] if o['id'] != update['id']]
                if update['side'] == 'buy':
                    self.my_bids[symbol] = \
                        [e for e in self.my_bids[symbol] if e['id'] != update['id']]
                    self.balance[quot]['free'] += update['amount'] * update['price']
                else:
                    self.my_asks[symbol] = \
                        [e for e in self.my_asks[symbol] if e['id'] != update['id']]
                    self.balance[coin]['free'] += update['amount']
            elif update['type'] == 'limit':
                if symbol in self.open_orders:
                    self.open_orders[symbol].append(update)
                else:
                    self.open_orders[symbol] = [update]
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
            new_balance[c] = {
                'free': float(e['free']),
                'used': float(e['locked']),
                'borrowed': float(e['borrowed']),
                'interest': float(e['interest']),
                'equity': float(e['netAsset']),
                'free': float(e['free']),
            }

            new_balance[c]['onhand'] = new_balance[c]['free'] + new_balance[c]['used']
            new_balance[c]['debt'] = new_balance[c]['interest'] + new_balance[c]['borrowed']
            if c not in self.all_coins_set:
                new_balance[c]['liquidate'] = True
                continue
            else:
                new_balance[c]['liquidate'] = False
            onhand_sum_quot += self.cm.convert_amount(new_balance[c]['onhand'], c, self.quot)
            debt_sum_quot += self.cm.convert_amount(new_balance[c]['debt'], c, self.quot)
            new_balance[c]['borrowable'] = (self.cm.convert_amount(borrowable_quot, self.quot, c)
                                            if c in self.do_borrow else 0.0)
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
        self.tradable_bnb = max(0.0, new_balance['BNB']['onhand'] - self.settings['bnb_buffer'])
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
            self.open_orders = {}
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
                try:
                    self.open_orders[oo['symbol']].append(oo)
                except(KeyError):
                    self.open_orders[oo['symbol']] = [oo]
        else:
            symbols = [symbol]
            open_orders = self.fetch_margin_open_orders(symbol)
            self.my_bids[symbol], self.my_asks[symbol] = [], []
            self.open_orders[symbol] = open_orders
            for oo in sorted(open_orders, key=lambda x: x['price']):
                if oo['side'] == 'buy':
                    self.my_bids[symbol].append(oo)
                else:
                    self.my_asks[symbol].append(oo)
        now = time()
        for symbol in symbols:
            self.time_keepers['update_open_orders'][symbol] = now

    def fetch_my_trades(self, symbol: str) -> [dict]:
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
        return my_trades

    def init_my_trades(self, symbol: str):
        my_trades = self.fetch_my_trades(symbol)
        self.set_analysis(symbol, my_trades)
        self.time_keepers['update_my_trades'][symbol] = time()

    def set_analysis(self, s: str, my_trades: [dict]):
        age_limit_millis = max(self.cc.milliseconds() - self.settings['max_memory_span_millis'],
                               self.settings['snapshot_timestamp_millis'])
        my_trades_cropped, analysis = analyze_my_trades(
            [mt for mt in my_trades if mt['timestamp'] > age_limit_millis]
        )
        analysis['long_cost_vol'], analysis['shrt_cost_vol'] = \
            calc_rolling_cost_vol(my_trades_cropped,
                                  analysis['small_big_amount_threshold'],
                                  (self.cc.milliseconds() -
                                   self.settings['millis_rolling_small_trade_window']))
        analysis['long_sel_price'] = round_up(
            analysis['true_long_vwap'] * self.settings['profit_pct_plus'],
            self.price_precisions[s]
        )
        analysis['shrt_buy_price'] = round_dn(
            analysis['true_shrt_vwap'] * self.settings['profit_pct_minus'],
            self.price_precisions[s]
        )
        self.my_trades[s] = my_trades_cropped
        self.my_trades_analyses[s] = analysis

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
            my_trades = self.my_trades[symbol]
            if new_my_trades:
                no_dash = symbol.replace('/', '_')
                cache_filepath = f'cache/binance/{self.user}/my_trades/{no_dash}/'
                condition = lambda x: x['id'] > self.my_trades[symbol][-1]['id']
                written_my_trades = self.write_cache(cache_filepath, new_my_trades, condition)
                my_trades += written_my_trades
            self.set_analysis(symbol, my_trades)
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
        amount = round_up(amount, 8)
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

    def set_ideal_orders(self, s: str):

        coin, quot = self.symbol_split[s]

        # prepare data
        other_bids = calc_other_orders(self.my_bids[s], self.cm.order_book[s]['bids'])
        other_asks = calc_other_orders(self.my_asks[s], self.cm.order_book[s]['asks'])

        highest_other_bid = \
            sorted(other_bids, key=lambda x: x['price'])[-1] if other_bids else self.my_bids[s][-1]
        lowest_other_ask = \
            sorted(other_asks, key=lambda x: x['price'])[0] if other_asks else self.my_asks[s][0]

        other_bid_incr = round(highest_other_bid['price'] + 10**-self.price_precisions[s],
                               self.price_precisions[s])
        other_ask_decr = round(lowest_other_ask['price'] - 10**-self.price_precisions[s],
                               self.price_precisions[s])

        small_trade_cost_default = max(self.min_trade_costs[s],
                                       (self.balance[quot]['account_equity'] *
                                        self.settings['account_equity_pct_per_trade']))
        small_trade_cost = max(
            10**-self.amount_precisions[s] * self.cm.last_price[s],
            small_trade_cost_default
        )

        self.d[s]['min_big_trade_cost'] = max(
            small_trade_cost * self.settings['min_big_trade_cost_multiplier'],
            self.my_trades_analyses[s]['small_big_amount_threshold'] * self.cm.last_price[s] * 1.1
        )


        # set ideal orders
        exponent = self.settings['entry_vol_modifier_exponent']
        if s in self.do_shrt_sel:
            shrt_sel_price = max([
                round_up(self.cm.max_ema[s] * self.settings['entry_spread_plus'],
                         self.price_precisions[s]),
                other_bid_incr,
                (other_ask_decr if small_trade_cost / other_ask_decr < lowest_other_ask['amount']
                 else lowest_other_ask['price'])
            ])
            shrt_amount_modifier = max(
                1.0,
                min(
                    self.settings['min_big_trade_cost_multiplier'] - 1,
                    (self.cm.last_price[s] /
                     self.my_trades_analyses[s]['shrt_buy_price'])**exponent
                )
            ) if self.my_trades_analyses[s]['shrt_buy_price'] > 0.0 else 1.0
            shrt_sel_amount = max(0.0, min(
                small_trade_cost * shrt_amount_modifier,
                (self.balance[quot]['account_equity'] *
                 self.settings['account_equity_pct_per_period'] * shrt_amount_modifier -
                 self.my_trades_analyses[s]['shrt_cost_vol'])) / shrt_sel_price)
            self.ideal_shrt_sel[s] = {
                'side': 'sell',
                'amount': (ssar if (ssar := round_up(shrt_sel_amount, self.amount_precisions[s])) *
                           shrt_sel_price >= self.min_trade_costs[s] else 0.0),
                'price': shrt_sel_price
            }
        if s in self.do_long_buy:
            long_buy_price = min([
                round_dn(self.cm.min_ema[s] * self.settings['entry_spread_minus'],
                         self.price_precisions[s]),
                other_ask_decr,
                (other_bid_incr if small_trade_cost / other_bid_incr < highest_other_bid['amount']
                 else highest_other_bid['price'])
            ])
            long_amount_modifier = max(
                1.0,
                min(
                    self.settings['min_big_trade_cost_multiplier'] - 1,
                    (self.my_trades_analyses[s]['long_sel_price'] /
                     self.cm.last_price[s])**exponent
                )
            )
            long_buy_amount = max(0.0, min(
                small_trade_cost * long_amount_modifier,
                (self.balance[quot]['account_equity'] *
                 self.settings['account_equity_pct_per_period'] * long_amount_modifier -
                 self.my_trades_analyses[s]['long_cost_vol'])) / long_buy_price)
            self.ideal_long_buy[s] = {
                'side': 'buy',
                'amount': (lbar if (lbar := round_up(long_buy_amount, self.amount_precisions[s])) *
                           long_buy_price >= self.min_trade_costs[s] else 0.0),
                'price': long_buy_price
            }
        if s in self.do_shrt_buy:
            shrt_buy_amount = round_up(self.my_trades_analyses[s]['true_shrt_amount'],
                                       self.amount_precisions[s])
            self.ideal_shrt_buy[s] = {
                'side': 'buy',
                'amount': shrt_buy_amount,
                'price': min([round_dn(self.cm.min_ema[s], self.price_precisions[s]),
                              other_ask_decr,
                              (other_bid_incr if shrt_buy_amount < highest_other_bid['amount']
                               else highest_other_bid['price']),
                              self.my_trades_analyses[s]['shrt_buy_price']])
            }
        if s in self.do_long_sel:
            long_sel_amount = round_up(self.my_trades_analyses[s]['true_long_amount'],
                                       self.amount_precisions[s])
            self.ideal_long_sel[s] = {
                'side': 'sell',
                'amount': long_sel_amount,
                'price': max([round_up(self.cm.max_ema[s], self.price_precisions[s]),
                              other_bid_incr,
                              (other_ask_decr if long_sel_amount < lowest_other_ask['amount']
                               else lowest_other_ask['price']),
                              self.my_trades_analyses[s]['long_sel_price']])
            }

    def set_liquidation_order(self, s: str):
        '''
        run async
        '''
        coin, quot = self.symbol_split[s]
        ticker = self.cc.fetch_ticker(s)
        ohlcv = self.cc.fetch_ohlcv(s, limit=1000)
        self.cm.init_ema(s)
        small_trade_cost = max(self.min_trade_costs[s],
                               (self.balance[quot]['account_equity'] *
                                self.settings['account_equity_pct_per_trade']))
        bid_price = round_dn(min(ticker['bid'], self.cm.min_ema[s]), self.price_precisions[s])
        ask_price = round_up(max(ticker['ask'], self.cm.max_ema[s]), self.price_precisions[s])

        bid_amount = round_up(small_trade_cost / bid_price, self.amount_precisions[s])
        ask_amount = round_up(small_trade_cost / ask_price, self.amount_precisions[s])
        if quot == self.quot:
            if self.balance[coin]['debt'] > 0.0:
                if self.balance[coin]['debt'] >= self.balance[coin]['onhand']:
                    repay_amount = self.balance[coin]['onhand']
                    ask_amount = 0.0
                else:
                    repay_amount = self.balance[coin]['debt']
                    bid_amount = 0.0
                    if self.balance[coin]['onhand'] - self.balance[coin]['debt'] < \
                            self.min_trade_costs[s] / ask_price:
                        ask_amount = 0.0
            else:
                repay_amount = 0.0
                bid_amount = 0.0
                if self.balance[coin]['onhand'] < self.min_trade_costs[s] / ask_price:
                    ask_amount = 0.0
        elif coin == self.quot:
            pass
        self.liquidation_orders[s] = []
        if bid_price and bid_amount:
            self.liquidation_orders[s].append(
                {'symbol': s, 'side': 'buy', 'amount': bid_amount, 'price': bid_price})
        if ask_price and ask_amount:
            self.liquidation_orders[s].append(
                {'symbol': s, 'side': 'sell', 'amount': ask_amount, 'price': ask_price})
        if repay_amount:
            self.liquidation_orders[s].append(
                {'symbol': s, 'side': 'repay', 'amount': repay_amount})

    def allocate_credit(self):
        # allocate credit and select eligible orders
        # default repay all debt
        # borrow to cover entries
        # if any exit is close to filling, borrow to cover exit(s)
        credit_available_quot = self.balance[self.quot]['borrowable'] * 0.995

        borrows = {c_: 0.0 for c_ in self.all_coins_set}
        coin_available = {c_: (self.tradable_bnb if c_ == 'BNB' else self.balance[c_]['onhand'])
                          for c_ in self.all_coins_set}

        long_buys = \
            [{**{'symbol': s_,
                 'lp_diff': (self.cm.last_price[s_] / self.ideal_long_buy[s_]['price'])},
              **self.ideal_long_buy[s_]} for s_ in self.ideal_long_buy
             if s_ in self.do_long_buy and self.ideal_long_buy[s_]['amount'] > 0.0]
        shrt_sels = \
            [{**{'symbol': s_,
                 'lp_diff': (self.ideal_shrt_sel[s_]['price'] / self.cm.last_price[s_])},
              **self.ideal_shrt_sel[s_]} for s_ in self.ideal_shrt_sel
             if s_ in self.do_shrt_sel and self.ideal_shrt_sel[s_]['amount'] > 0.0]
        entries = sorted(long_buys + shrt_sels, key=lambda x: x['lp_diff'])
        eligible_entries = []
        for entry in entries:
            c_, q_ = self.symbol_split[entry['symbol']]
            if entry['side'] == 'sell':
                if (diff := entry['amount'] - coin_available[c_]) > 0.0:
                    # not enough coin onhand, borrow

                    to_borrow = min(credit_available_quot / entry['price'], diff)
                    borrows[c_] += to_borrow
                    credit_available_quot -= to_borrow * entry['price']
                    if coin_available[c_] + to_borrow >= entry['amount']:
                        eligible_entries.append(entry)
                        coin_available[c_] += (to_borrow - entry['amount'])
                else:
                    eligible_entries.append(entry)
                    coin_available[c_] -= entry['amount']
            else:
                entry_cost = entry['amount'] * entry['price']
                if (diff := entry_cost - coin_available[q_]) > 0.0:
                    to_borrow = min(credit_available_quot, diff)
                    borrows[q_] += to_borrow
                    credit_available_quot -= to_borrow
                    if coin_available[q_] + to_borrow >= entry_cost:
                        eligible_entries.append(entry)
                        coin_available[q_] += (to_borrow - entry_cost)
                else:
                    eligible_entries.append(entry)
                    coin_available[q_] -= entry_cost
        shrt_buys = \
            [{**{'symbol': s_,
                 'lp_diff': (self.cm.last_price[s_] / self.ideal_shrt_buy[s_]['price'])},
              **self.ideal_shrt_buy[s_]} for s_ in self.ideal_shrt_buy
             if (self.ideal_shrt_buy[s_]['amount'] *
                 self.ideal_shrt_buy[s_]['price']) > self.d[s_]['min_big_trade_cost']]
        long_sels = \
            [{**{'symbol': s_,
                 'lp_diff': (self.ideal_long_sel[s_]['price'] / self.cm.last_price[s_])},
              **self.ideal_long_sel[s_]} for s_ in self.ideal_long_sel
             if (self.ideal_long_sel[s_]['amount'] *
                 self.ideal_long_sel[s_]['price']) > self.d[s_]['min_big_trade_cost']]
        exits = sorted(long_sels + shrt_buys, key=lambda x: x['lp_diff'])
        eligible_exits = []
        for exit in exits:
            s_ = exit['symbol']
            c_, q_ = self.symbol_split[s_]
            if exit['lp_diff'] == 0.0 or exit['lp_diff'] > 1.2:
                continue
            if exit['side'] == 'sell':
                if (diff := exit['amount'] - coin_available[c_]) > 0.0:
                    # not enough coin for full long exit
                    if (credit_available_coin := credit_available_quot / exit['price']) < diff:
                        # we do partial exit
                        partial_exit_cost = \
                            credit_available_quot + coin_available[c_] * exit['price']
                        if partial_exit_cost >= self.d[s_]['min_big_trade_cost']:
                            exit['partial_amount'] = round_dn(
                                partial_exit_cost / exit['price'],
                                self.amount_precisions[s_])
                            borrows[c_] += credit_available_coin
                            credit_available_quot = 0.0
                            coin_available[c_] = 0.0
                            eligible_exits.append(exit)
                    else:
                        # we do full exit
                        eligible_exits.append(exit)
                        coin_available[c_] += (diff - exit['amount'])
                        borrows[c_] += diff
                        credit_available_quot -= diff * exit['price']
                else:
                    exit['partial_amount'] = max(
                        round_dn(coin_available[c_], self.amount_precisions[s_]),
                        self.ideal_long_sel[s_]['amount']
                    )
                    eligible_exits.append(exit)
                    coin_available[c_] -= exit['partial_amount']
            else:
                exit_cost = exit['amount'] * exit['price']
                if (diff := exit_cost - coin_available[q_]) > 0.0:
                    if credit_available_quot < diff:
                        # we do partial exit
                        partial_exit_cost = coin_available[q_] + credit_available_quot
                        if partial_exit_cost >= self.d[s_]['min_big_trade_cost']:
                            exit['partial_amount'] = round_dn(
                                (coin_available[q_] + credit_available_quot) / exit['price'],
                                self.amount_precisions[s_]
                            )
                            eligible_exits.append(exit)
                            borrows[q_] += credit_available_quot
                            credit_available_quot = 0.0
                            coin_available[q_] = 0.0
                    else:
                        # we do full exit with borrowed quot
                        eligible_exits.append(exit)
                        credit_available_quot -= diff
                        coin_available[q_] += (diff - exit_cost)
                        borrows[q_] += diff
                else:
                    # we do full shrt exit with quot onhand
                    if self.balance[c_]['debt'] > exit['amount']:
                        exit['partial_amount'] = min(
                            round_dn(coin_available[q_] / max(exit['price'], 9e-9),
                                     self.amount_precisions[s_]),
                            round_up(self.balance[c_]['debt'], self.amount_precisions[s_])
                        )
                        coin_available[q_] -= (exit['partial_amount'] * exit['price'])
                    else:
                        coin_available[q_] -= exit_cost
                    eligible_exits.append(exit)
        self.eligible_entries = [{k: e[k] for k in ['symbol', 'side', 'amount', 'price']}
                                 for e in eligible_entries]
        pa = 'partial_amount'
        self.eligible_exits = [{k.replace('partial_', ''): e[k]
                                for k in ['symbol', 'side', (pa if pa in e else 'amount'), 'price']}
                               for e in eligible_exits]

        for c_ in borrows:
            borrows[c_] -= min(self.balance[c_]['debt'], coin_available[c_])
            self.ideal_borrow[c_] = max(0.0, borrows[c_])
            self.ideal_repay[c_] = max(0.0, -borrows[c_])

    def execute_to_exchange(self):
        now = time()
        if now - self.prev_execution_ts < 1.0: # min 1 second between executions to exchange
            return
        self.counter += 1
        self.try_wrapper(self.allocate_credit)
        self.prev_execution_ts = time()
        if any([self.time_keepers['update_my_trades'][s] < 11 for s in self.symbols]) or \
                any([self.time_keepers['update_open_orders'][s] < 11 for s in self.symbols]) or \
                self.time_keepers['update_balance'] < 11:
            return # don't execute if any unfinished updates of my_trades, open_orders or balance

        if self.future_handling_lock.locked():
            if time() - self.time_keepers['future_handling_lock'] > self.force_lock_release_timeout:
                self.future_handling_lock.release()
            else:
                return # don't execute if unfinished previous execution

        future_results = []

        now_millis = self.cc.milliseconds()
        if now_millis - self.prev_loan_ts['all'] > 1000 * 2: # min 2 sec between borrow/repay
            for coin in self.all_coins_set:
                if now_millis - self.prev_loan_ts[coin] < 1000 * 60 * 2:
                    continue # min 2 min between consecutive same coin borrow/repay
                amount = self.ideal_borrow[coin]
                if amount > 0.0 and amount <= self.balance[coin]['borrowable']:
                    future_results.append((coin, self.borrow, threaded(self.borrow)(coin, amount)))
                    self.ideal_borrow[coin] = 0.0
                    self.prev_loan_ts[coin] = now_millis
                    self.prev_loan_ts['all'] = now_millis
                    break
                amount = self.ideal_repay[coin]
                if amount > 0.0 and amount <= self.balance[coin]['free'] and \
                        now_millis - self.prev_repay_ts[coin] > 59 * 60 * 1000:
                    # min 59 min between consecutive same coin repay
                    future_results.append((coin, self.repay, threaded(self.repay)(coin, amount)))
                    self.ideal_repay[coin] = 0.0
                    self.balance[coin]['debt'] -= amount
                    self.prev_loan_ts[coin] = now_millis
                    self.prev_loan_ts['all'] = now_millis
                    self.prev_repay_ts[coin] = now_millis
    
                    break

        order_deletions, order_creations = \
            filter_orders(flatten(self.open_orders.values()),
                          self.eligible_entries + self.eligible_exits)
        lod, loc = len(order_deletions), len(order_creations)
        if any([lod, loc]):
            print('deletion queue', lod, 'creation queue', loc)
        else:
            if self.counter % 21 == 0:
                print('deletion queue', lod, 'creation queue', loc)
        for o in order_deletions[:4]:
            if self.cm.can_execute():
                self.cm.add_ts_to_lists()
                fn = self.cancel_margin_order
                future_results.append(
                    (o['symbol'], fn, threaded(fn)(o['id'], o['symbol'])))
        k = 0
        for o in order_creations:
            if k >= 3:
                break
            if self.cm.can_execute():
                if o['side'] == 'buy':
                    if o['amount'] * o['price'] > self.balance[self.quot]['free']:
                        continue
                else:
                    if o['amount'] > self.balance[self.symbol_split[o['symbol']][0]]['free']:
                        continue
                self.cm.add_ts_to_lists()
                fn = getattr(self, f"create_margin_{'bid' if o['side'] == 'buy' else 'ask'}")
                future_results.append(
                  (o['symbol'], fn, threaded(fn)(o['symbol'], o['amount'], o['price'])))
                k += 1

        threaded(self.handle_future_results)(future_results)

    def handle_future_results(self, future_results: [tuple]):
        try:
            if future_results is None:
                return
            self.future_handling_lock.acquire()
            self.time_keepers['future_handling_lock'] = time()
            for symbol, fn, result in future_results:
                if result.exception():
                    if 'Unknown order sent' in result.exception().args[0]:
                        print_(['ERROR', 'margin', symbol,
                               'trying to cancel non-existing order; schedule open orders update'])
                        self.time_keepers['update_open_orders'][symbol] = 0
                    elif 'Account has insufficient balance' in result.exception().args[0]:
                        print_(['ERROR', 'margin', symbol, fn.__name__,
                                'insufficient funds; schedule balance update'])
                        self.time_keepers['update_balance'] = 0
                    elif 'Balance is not enough' in result.exception().args[0]:
                        print_(["ERROR", 'margin', symbol, fn.__name__,
                                'balance not enough; schedule balance update'])
                        self.time_keepers['update_balance'] = 0
                    else:
                        print_(['ERROR', 'margin', symbol, result.exception(),
                                fn.__name__])
                else:
                    self.try_wrapper(self.update_after_execution, (result.result(),))
            self.future_handling_lock.release()
        except(Exception) as e:
            print('ERROR with handle_future_results', e)

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
                if shrt_amount <= 0.0 or shrt_cost <= 0.0:
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
                if long_amount <= 0.0 or long_cost <= 0.0:
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

    start_ts = min(long_start_ts, shrt_start_ts) - 1000 * 60 * 60 * 24
    _, cropped_my_trades = partition_sorted(my_trades, lambda x: x['timestamp'] >= start_ts)
    return cropped_my_trades, analysis
