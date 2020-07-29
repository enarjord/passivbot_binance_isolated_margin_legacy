from common_procedures import init_binance_client, threaded, init_binance_ccxt
from common_functions import ts_to_date, sort_dict_keys, calc_new_ema, flatten
from binance.websockets import BinanceSocketManager
from typing import Callable
from time import time, sleep
from commons import Commons
from vwap import Vwap
from streamer import Streamer
import sys
import json


def load_settings(user: str):
    default_settings = json.load(open('settings/binance/default.json'))
    try:
        settings = json.load(open(f'settings/binance/{user}.json'))
        for k0 in default_settings:
            if k0 not in settings:
                settings[k0] = default_settings[k0]
    except(FileNotFoundError):
        print(f'{user} not found, using default settings')
        settings = default_settings
    settings['user'] = user
    return settings


def prepare_bot(exchange: str, user: str):
    settings = load_settings(user)
    commons = Commons(user, settings['ema_spans_minutes'])
    all_coins = set(flatten([s.split('/') for s in commons.cc.markets]))
    all_margin_pairs = [f"{e['base']}/{e['quote']}" for e in commons.cc.sapi_get_margin_allpairs()]
    settings['symbols'] = [s for c in settings['coins']
                           if (s := f"{c}/{settings['quot']}") in all_margin_pairs]
    commons.init(settings['symbols'])
    receiver_funcs = [commons.update_ema]
    vwap = Vwap(commons, settings)
    vwap.init()
    receiver_funcs.append(vwap.on_update)
    commons.start_call_limiter()
    streamer = Streamer(settings, receiver_funcs)
    return commons, vwap, streamer


def main():
    user = sys.argv[1]
    commons, vwap, streamer = prepare_bot('binance', user)
    streamer.start()

if __name__ == '__main__':
    main()
