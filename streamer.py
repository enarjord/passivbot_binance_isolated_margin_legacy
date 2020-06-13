from time import sleep, time
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import \
    BinanceWebSocketApiManager
import json



class Streamer:
    '''
    manages stream and order book, calls receiver funcs upon update
    '''
    def __init__(self,
                 hyperparams: dict,
                 receiver_funcs: list):
        self.receiver_funcs = receiver_funcs
        self.symbols = hyperparams['symbols']
        self.symbol_formatting_map = {s.replace('/', '').lower(): s for s in self.symbols}
        self.depth_levels = 5
        self.order_book = {s: {'bids': [{'price': 0.0, 'amount': 0.0}] * self.depth_levels,
                          'asks': [{'price': 0.0, 'amount': 0.0}] * self.depth_levels}
                      for s in self.symbols}
        self.stream_tick_ts = 0
        self.binance_websocket_api_manager = None

    def receive_update(self, msg):
        update = self.format_msg(msg)
        if update is None:
            return
        self.order_book[update['s']]['bids'] = update['bids']
        self.order_book[update['s']]['asks'] = update['asks']
        for receiver_func in self.receiver_funcs:
            receiver_func(update['s'], update)
        self.stream_tick_ts = time()

    def start(self, do_print: bool = False):
        self.symbol_formatting_map = {
            s.replace('/', '').lower() + f'@depth{self.depth_levels}': s
            for s in self.symbols
        }
        print(self.symbol_formatting_map)
        self.binance_websocket_api_manager = BinanceWebSocketApiManager(exchange="binance.com")
        self.binance_websocket_api_manager.create_stream(
            ['depth5'], [s.replace('/', '') for s in self.symbols]
        )
        while True:
            oldest_stream_data_from_stream_buffer = \
                self.binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
            if oldest_stream_data_from_stream_buffer:
                self.receive_update(oldest_stream_data_from_stream_buffer)

    def format_msg(self, msg):
        if msg is not None:
            msg = json.loads(msg)
            if 'data' in msg:
                return {'s': self.symbol_formatting_map[msg['stream']],
                        'bids': sorted([{'price': float(e[0]), 'amount': float(e[1])}
                                         for e in msg['data']['bids']], key=lambda x: x['price']),
                        'asks': sorted([{'price': float(e[0]), 'amount': float(e[1])}
                                         for e in msg['data']['asks']], key=lambda x: x['price']),
                        'lastUpdateId': msg['data']['lastUpdateId']}

    def printer_(self):
        space_per_element = 18
        symbols_per_line = 11
        symbols = sorted(self.symbols)
        print(ts_to_date(time()))
        for k in range(0, len(symbols) + 1, symbols_per_line):
            symbols_group = symbols[k:k + symbols_per_line]
            if symbols_group == []:
                break
            lines = [''.join(['{:<{n}}'.format(s, n=space_per_element) for s in symbols_group])]
            for i in range(self.depth_levels - 1, -1, -1):
                line = ''
                for s in symbols_group:
                    line += '{:<{n}}'.format(
                        self.order_book[s]['asks'][i]['price'], n=space_per_element)
                lines.append(line)
            lines.append('-' * space_per_element * len(symbols_group))
            for i in range(self.depth_levels - 1, -1, -1):
                line = ''
                for s in symbols_group:
                    line += '{:<{n}}'.format(
                        self.order_book[s]['bids'][i]['price'], n=space_per_element)
                lines.append(line)
            lines.append('')
            for line in lines:
                print(line)
            print()


def main():
    rf = lambda s, msg: print(s, msg)
    streamer = Streamer({'symbols': ['ETH/BTC', 'EOS/BTC', 'BNB/BTC', 'LTC/BTC', 'LINK/BTC']}, [rf])
    streamer.start()

if __name__ == '__main__':
    main()
