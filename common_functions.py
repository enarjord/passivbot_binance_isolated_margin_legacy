from math import ceil, floor
import pandas as pd
import datetime
from typing import Callable


def ts_to_date(timestamp: float) -> str:
    return str(datetime.datetime.fromtimestamp(timestamp)).replace(' ', 'T')


def flatten(lst: list) -> list:
    '''
    flattens list of lists
    '''
    return [y for x in lst for y in x]


def round_up(n: float, d: int = 0):
    return ceil(n * 10**d) / (10**d)


def round_down(n: float, d: int = 0):
    return floor(n * 10**d) / (10**d)


def round_dn(n: float, d: int = 0):
    return floor(n * 10**d) / (10**d)


def increment_by_precision(val: float, precision: float) -> float:
    return round(val + 10**-precision, precision)


def decrement_by_precision(val: float, precision: float) -> float:
    return round(val - 10**-precision, precision)


def calc_new_ema(prev_val: float,
                 new_val: float,
                 prev_ema: float,
                 span: float = None,
                 alpha: float = None,
                 n_steps: int = 1) -> float:
    if n_steps <= 0:
        return prev_ema
    if alpha is None:
        if span is None:
            raise Exception('please specify alpha or span')
        alpha = 2 / (span + 1)
    if n_steps == 1:
        return prev_ema * (1 - alpha) + new_val * alpha
    else:
        return calc_new_ema(prev_val,
                            new_val,
                            prev_ema * (1 - alpha) + prev_val * alpha,
                            alpha=alpha,
                            n_steps=n_steps - 1)


def ts_to_day(timestamp: float) -> str:
    return str(datetime.datetime.fromtimestamp(timestamp))[:10]


def ts_to_date(timestamp: float) -> str:
    return str(datetime.datetime.fromtimestamp(timestamp)).replace(' ', 'T')


def sort_and_drop_duplicates_by_index(df1: pd.DataFrame, df2: pd.DataFrame = None) -> pd.DataFrame:
    if df1 is None and df2 is None:
        return None
    df = pd.concat([df1, df2])
    df = df.sort_index()
    return df.loc[~df.index.duplicated()]


def remove_elem(lst: [], elem):
    '''
    returns new list, first occurrence of elem removed
    '''
    try:
        i = lst.index(elem)
        return lst[:i] + lst[i + 1:]
    except(ValueError):
        return lst
    

def sort_dict_keys(d):
    if type(d) == list:
        return [sort_dict_keys(e) for e in d]
    if type(d) != dict:
        return d
    return {key: sort_dict_keys(d[key]) for key in sorted(d)}


def strs_to_floats(d):
    if type(d) == list:
        return list(map(strs_to_floats, d))
    if type(d) == bool:
        return d
    if type(d) != dict:
        try:
            return float(d)
        except(ValueError):
            return d
    return {key: strs_to_floats(d[key]) for key in d}


def remove_duplicates(dict_list: [dict], key: str = None, sort: bool = False) -> [dict]:
    try:
        seen = set()
        dup_removed = []
        if key is not None:
            dict_list_ = sorted(dict_list, key=lambda x: x['id']) if sort else dict_list
            for d in dict_list_:
                if d[key] not in seen:
                    dup_removed.append(d)
                    seen.add(d[key])
        else:
            if sort:
                print('warning: will not sort dict_list without key')
            for d in dict_list:
                d_items = tuple(sorted(d.items()))
                if d_items not in seen:
                    dup_removed.append(d)
                    seen.add(d_items)
        return dup_removed
    except:
        print('error in remove duplicates')
        print(dict_list)
        raise Exception


def partition_sorted(lst: list, condition: Callable):
    '''
    takes sorted list
    returns tuple: (list0 where condition == False and list1 where condition == True)
    '''
    for i in range(len(lst)):
        if condition(lst[i]):
            return lst[:i], lst[i:]
    return lst, []


