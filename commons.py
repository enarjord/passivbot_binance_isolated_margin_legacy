from time import time, sleep
from common_functions import calc_new_ema
from common_procedures import threaded, init_binance_ccxt
import sys


class Commons:
    '''
    manages emas and call limiting
    keeps updated order book
    maintains conversion_costs
    '''
    def __init__(self, user: str, ema_spans_minutes: [int]):
        self.cc = init_binance_ccxt(user)
        self.cc.load_markets()
        self.cc.options['warnOnFetchOpenOrdersWithoutSymbol'] = False
        self.ema_spans_minutes = ema_spans_minutes
        self.minutes_to_seconds = {span: span * 60 for span in ema_spans_minutes}

        self.symbol_split = {s: s.split('/') for s in self.cc.markets}

        self.rolling_secondly_order_calls = []
        self.rolling_minutely_order_calls = []
        self.max_orders_per_day = 200000
        self.max_orders_per_minute = int(self.max_orders_per_day / (60 * 24))
        self.max_orders_per_second = 10
        self.running_order_ts_flusher = False

        self.order_book = {}
        self.previous_updates = {}
        self.emas = {}
        self.min_ema = {}
        self.max_ema = {}
        self.ema_second = {}
        self.last_price = {}
        self.max_bid = {}
        self.min_ask = {}

        self.conversion_costs = {}
        self.fee = 0.99925

        self.min_trade_amounts = {symbol: self.cc.markets[symbol]['limits']['cost']['min']
                                  for symbol in self.cc.markets}
        self.price_precisions = {symbol: self.cc.markets[symbol]['precision']['price']
                                 for symbol in self.cc.markets}
        self.amount_precisions = {symbol: self.cc.markets[symbol]['precision']['amount']
                                  for symbol in self.cc.markets}


    def init(self, symbols: [str]):
        print()
        calls = []
        for i, symbol in enumerate(symbols):
            sys.stdout.write(f'\r{i + 1} / {len(symbols)} calculating ema for {symbol} ... ')
            sys.stdout.flush()
            calls.append(threaded(self.init_ema)(symbol))
            sleep(0.1)
        [call.result() for call in calls]
        print()
        tickers = self.cc.fetch_tickers()
        self.symbol_split = {s: s.split('/') for s in symbols}
        self.conversion_costs = init_conversion_costs({s: tickers[s] for s in symbols}, self.fee)
        self.order_book = {s: {'bids': [{'price': tickers[s]['bid'],
                                         'amount': tickers[s]['bidVolume']}],
                               'asks': [{'price': tickers[s]['ask'],
                                         'amount': tickers[s]['askVolume']}]}
                           for s in symbols}
        self.previous_updates = self.order_book.copy()


    def init_ema(self, symbol):
        ohlcv = self.cc.fetch_ohlcv(symbol, params={'limit': 1000})
        self.emas[symbol] = {}
        for span in self.ema_spans_minutes:
            ema = ohlcv[0][4]
            alpha = 2 / (span + 1)
            alpha_ = 1 - alpha
            for e in ohlcv[1:]:
                ema = ema * alpha_ + e[4] * alpha
            self.emas[symbol][span] = ema
        self.min_ema[symbol] = min(self.emas[symbol].values())
        self.max_ema[symbol] = max(self.emas[symbol].values())
        self.ema_second[symbol] = int(time())
        self.last_price[symbol] = ohlcv[-1][4]
        self.previous_updates[symbol] = {'bids': [{'amount': 0.0, 'price': ohlcv[-1][4]}],
                                         'asks': [{'amount': 0.0, 'price': ohlcv[-1][4]}]}

    def update_ema(self, symbol: str, update: dict):
        self.previous_updates[symbol] = self.order_book[symbol]
        self.order_book[symbol] = update
        now_second = int(time())
        if now_second == self.ema_second:
            return
        bid_ask_avg = (update['bids'][-1]['price'] + update['asks'][0]['price']) / 2
        coin, base = self.symbol_split[symbol]
        self.conversion_costs[coin][base] = self.fee * update['bids'][-1]['price']
        self.conversion_costs[base][coin] = self.fee / update['asks'][0]['price']
        for span in self.ema_spans_minutes:
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
        self.last_price[symbol] = bid_ask_avg


    def convert_amount(self, amount: float, coin_from: str, coin_to: str):
        try:
            return amount * self.conversion_costs[coin_from][coin_to]
        except(KeyError):
            return (amount * self.conversion_costs[coin_from]['BTC'] *
                    self.conversion_costs['BTC'][coin_to])


    ############################################################################################

    def start_call_limiter(self, interval: float = 0.1):
        def f():
            while self.running_order_ts_flusher:
                self.flush_order_timestamps()
                sleep(interval)
        if not self.running_order_ts_flusher:
            self.running_order_ts_flusher = True
            threaded(f)()

    def stop_call_limiter(self):
        self.running_order_ts_flusher = False

    def can_execute(self, n_orders: int = 1):
        if len(self.rolling_secondly_order_calls) + n_orders > self.max_orders_per_second:
            return False
        if len(self.rolling_minutely_order_calls) + n_orders > self.max_orders_per_minute:
            return False
        return True

    def add_ts_to_lists(self, n: int = 1):
        now = time()
        for _ in range(n):
            self.rolling_minutely_order_calls.append(now)
            self.rolling_secondly_order_calls.append(now)

    def flush_order_timestamps(self):

        def new_list(timestamps: list, ts_limit: float):
            for i, ts in enumerate(timestamps):
                if ts > ts_limit:
                    return timestamps[i:]
            return []

        now = time()
        self.rolling_minutely_order_calls = new_list(self.rolling_minutely_order_calls, now - 60.0)
        self.rolling_secondly_order_calls = new_list(self.rolling_secondly_order_calls, now - 1.0)


def init_conversion_costs(tickers: dict, fee: float = 0.99925):
    conversion_costs = {}
    for symbol in tickers:
        if not tickers[symbol]['ask'] or not tickers[symbol]['bid']:
            continue
        coin, base = symbol.split('/')
        if coin in conversion_costs:
            conversion_costs[coin][base] = fee * tickers[symbol]['bid']
        else:
            conversion_costs[coin] = {base: fee * tickers[symbol]['bid']}
            conversion_costs[coin][coin] = 1.0
        if base in conversion_costs:
            conversion_costs[base][coin] = fee / tickers[symbol]['ask']
        else:
            conversion_costs[base] = {coin: fee / tickers[symbol]['ask']}
            conversion_costs[base][base] = 1.0
    return conversion_costs


def convert_amount(amount: float,
                   cfrom: str,
                   cto: str,
                   conversion_costs: dict,
                   middlemen=['ETH', 'BTC', 'USDC', 'USDT']):
    if cto in conversion_costs[cfrom]:
        return amount * conversion_costs[cfrom][cto]
    if cfrom == cto:
        return amount
    for middleman in middlemen:
        if middleman in conversion_costs[cfrom]:
            return convert_amount(
                amount * conversion_costs[cfrom][middleman],
                middleman,
                cto,
                conversion_costs,
                remove_elem(middlemen, middleman))
    print(amount, cfrom, cto)
    raise(Exception)


def update_conversion_costs():
    pass


def calc_other_orders(my_orders: [dict],
                      order_book: [dict]) -> [dict]:
    # my_orders = [{'price': float, 'amount': float}]
    other_orders = {o['price']: o['amount'] for o in order_book}
    for o in my_orders:
        if o['price'] in other_orders:
            if o['amount'] >= other_orders[o['price']]:
                del other_orders[o['price']]
            else:
                other_orders[o['price']] = round(other_orders[o['price']] - o['amount'], 8)
    return [{'price': p, 'amount': other_orders[p]}
            for p in sorted(other_orders)]


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

