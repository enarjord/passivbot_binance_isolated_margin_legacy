import json
import os
from common_functions import ts_to_date
from time import time
import sys


def make_get_filepath(filepath: str) -> str:
    '''
    if not is path, creates dir and subdirs for path, returns path
    '''
    dirpath = os.path.dirname(filepath) if filepath[-1] != '/' else filepath
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    return filepath


def load_key_secret(exchange: str, user: str) -> (str, str):
    try:
        return json.load(open(f'api_key_secrets/{exchange}/{user}.json'))
    except(FileNotFoundError):
        print(f'\n\nPlease specify {exchange} API key/secret in file\n\napi_key_secre' + \
              f'ts/{exchange}/{user}.json\n\nformatted thus:\n["Ktnks95U...", "yDKRQqA6..."]\n\n')
        raise Exception('api key secret missing')


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


def load_settings(exchange: str, user: str):
    default_settings = json.load(open(f'settings/{exchange}/default.json'))
    try:
        settings = json.load(open(f'settings/{exchange}/{user}.json'))
        for k0 in default_settings:
            if k0 not in settings:
                settings[k0] = default_settings[k0]
    except FileNotFoundError:
        print(f'{user} not found, using default settings')
        settings = default_settings
    settings['user'] = user
    return settings