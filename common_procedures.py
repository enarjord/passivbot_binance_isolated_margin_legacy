from threading import Thread
from concurrent.futures import Future
from binance.client import Client
import ccxt
import json
import os
from common_functions import ts_to_date
from time import time
import sys


def make_get_filepath(filepath: str) -> None:
    '''
    if not is path, creates dir and subdirs for path, returns path
    '''
    dirpath = os.path.dirname(filepath) if filepath[-1] != '/' else filepath
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    return filepath


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future, args=(fn, future, args, kwargs)).start()
        return future
    return wrapper


def load_key_secret(exchange: str, user: str) -> (str, str):
    try:
        return json.load(open(f'api_key_secrets/{exchange}/{user}.json'))
    except(FileNotFoundError):
        print(f'\n\nPlease specify {exchange} API key/secret in file\n\napi_key_secre' + \
              f'ts/{exchange}/{user}.json\n\nformatted thus:\n["Ktnks95U...", "yDKRQqA6..."]\n\n')
        raise Exception('api key secret missing')


def make_get_filepath(filepath: str) -> None:
    '''
    if not is path, creates dir and subdirs for path, returns path
    '''
    dirpath = os.path.dirname(filepath) if filepath[-1] != '/' else filepath
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    return filepath


def init_binance_ccxt(user: str = 'e'):
    key_secret = load_key_secret('binance', user)
    cc = ccxt.binance()
    cc.options
    cc.apiKey = key_secret[0]
    cc.secret = key_secret[1]
    return cc


def init_ccxt(exchange: str, user: str = 'e'):
    key_secret = load_key_secret(exchange, user)
    cc = getattr(ccxt, exchange)()
    cc.apiKey = key_secret[0]
    cc.secret = key_secret[1]
    return cc


def init_binance_client(user: str) -> Client:
    key_secret = load_key_secret('binance', user)
    return Client(key_secret[0], key_secret[1])


def print_(args, r=False):
    line = ts_to_date(time())[:19] + '  '
    str_args = '{} ' * len(args)
    line += str_args.format(*args)
    if r:
        sys.stdout.write('\r' + line + '   ')
    else:
        print(line)
    sys.stdout.flush()
    return line

