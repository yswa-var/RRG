import hashlib
import os
import re
import string
from datetime import datetime, date
from functools import wraps
from random import randint, SystemRandom
from time import time, sleep

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

SLEEP = 0.07
MINUTE = 'minute'
DAY = 'day'
WEEK = 'week'
MONTH = 'month'

UPDATE_TIME = 'last_updated'
API_KEY = 'api_key'
API_SECRET = 'api_secret'
API_REDIRECT_URL = 'api_redirect_url'
ACCESS_TOKEN = 'api_access_token'
ACCESS_TOKEN_TIME = 'api_token_time'
DEFAULT_VENDOR = 'api'

interval_map = {
    '1min': MINUTE,
    '3min': '3minute',
    '5min': '5minute',
    '10min': '10minute',
    '15min': '15minute',
    '30min': '30minute',
    '60min': '60minute',
    'D': DAY,
    'W': WEEK,
    'MS': MONTH
}

# Make a regular expression for validating an Email
regex_email = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'  # better
# option 2 regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
regex_ph = re.compile(r'(^[+0-9]{1,3})*([0-9]{10,11}$)')


# option 2 r'^(?:\+?44)?[07]\d{9,13}$'


def is_eod_interval(interval):
    if interval in [DAY, WEEK, MONTH]:
        return True
    else:
        return False


class TimeDeltas:
    """
    Time related deltas
    """
    begin_delta = relativedelta(hour=8, minute=59, second=10, microsecond=0)
    pre_market_delta = relativedelta(hour=9, minute=7, second=15, microsecond=0)
    pre_market_wait = relativedelta(hour=9, minute=9, second=20, microsecond=0)
    start_delta = relativedelta(hour=9, minute=8, second=50, microsecond=0)
    today_open_delta = relativedelta(days=0, hour=9, minute=15, second=0, microsecond=0)
    today_open_delta_boom = relativedelta(days=0, hour=10, minute=30, second=0, microsecond=0)
    end_delta = relativedelta(hour=15, minute=35, second=0, microsecond=0)
    pre_rsi = relativedelta(hour=15, minute=45)
    day_end_delta = relativedelta(hour=23, minute=59, second=59)
    minute_second_reset_delta = relativedelta(minutes=1, second=0, microsecond=0)
    broadcast_candle_delta = relativedelta(minutes=1, second=0, microsecond=0)
    broadcast_delta = relativedelta(seconds=7)
    candle_delta = relativedelta(seconds=3)
    live_comp_delta = relativedelta(seconds=9)

    @staticmethod
    def back_delta(days=1):
        return relativedelta(days=days)

    @staticmethod
    def hist_delta(days=0):
        return relativedelta(days=days, hour=2, minute=0, second=0, microsecond=0)

    @staticmethod
    def day_delta(days=1):
        return relativedelta(days=days, hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def market_open_delta(days=0):
        return relativedelta(days=days, hour=9, minute=15, second=0, microsecond=0)

    @staticmethod
    def market_close_delta(days=0):
        return relativedelta(days=days, hour=15, minute=30, second=0, microsecond=0)


def is_null(value):
    if pd.isna(value) or value == '':
        return True
    else:
        return False


def chunk_dates(start: datetime, end: datetime, interval: str, as_timestamp=False, as_range=False) -> list:
    # noinspection PyTypeChecker
    start = pd.Timestamp(start).to_pydatetime()
    # noinspection PyTypeChecker
    end = pd.Timestamp(end).to_pydatetime()

    if start > end:
        start, end = end, start

    if start == end:
        result = [start, end]
    else:
        date_ranges = pd.date_range(start, end, freq=interval).to_pydatetime().tolist()
        if len(date_ranges) > 0:
            date_ranges = date_ranges + [end] if date_ranges[-1] != end else date_ranges
        else:
            date_ranges = [start, end]
        result = date_ranges

    if as_timestamp:
        result = [_dt.timestamp() for _dt in result]

    if as_range:
        result = list(zip(result[:-1], result[1:]))

    return result


def cartesian(iter1, iter2, names: tuple = ('iter1', 'iter2')) -> pd.DataFrame:
    iter1_df = pd.DataFrame({'iter1': iter1})
    iter2_df = pd.DataFrame({'iter2': iter2})
    iter1_df['c'] = 1
    iter2_df['c'] = 1

    result = pd.merge(iter1_df, iter2_df, how='inner', on='c')
    result.drop(columns=['c'], inplace=True)
    result.rename(columns={'iter1': names[0], 'iter2': names[1]}, inplace=True)
    return result


def cartesian_df(iter1_df: pd.DataFrame, iter2_df: pd.DataFrame, names: tuple = ('_1', '_2')) -> pd.DataFrame:
    iter1_df['c'] = 1
    iter2_df['c'] = 1

    result = pd.merge(iter1_df, iter2_df, how='inner', on='c', suffixes=names)
    result.drop(columns=['c'], inplace=True)
    return result


def clean_db_cols(db_df: pd.DataFrame) -> pd.DataFrame:
    cols_to_none = ['open', 'high', 'low', 'close', 'volume', 'oi']
    final = db_df.copy()
    for _col in cols_to_none:
        if _col in final.columns:
            final.loc[pd.isna(final[_col]), _col] = None
            final[_col].replace([np.NaN], None, inplace=True)
    return final


def clean_db_dict(db_data: list) -> list:
    cols_to_none = ['open', 'high', 'low', 'close', 'volume', 'oi']
    final = []
    for _latest in db_data:
        _cleaned = {}
        for _key, _value in _latest.items():
            if _key in cols_to_none:
                _value = _value if not pd.isna(_value) else None
            _cleaned[_key] = _value
        final.append(_cleaned)
    return final


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def extract_latest_xref(latest_xref: dict, entity_id, key: str, default_value=None, data_type=None):
    entity_data = latest_xref.get(entity_id, {})
    key_value = entity_data.get(key, default_value)
    if not is_null(key_value) and not is_null(data_type):
        key_value = data_type(key_value)
    return key_value


def random_with_n_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


def remove_from_str(value, remove=','):
    return str(value).replace(remove, '')


def is_true(value):
    return value in [True, 'YES', 'Yes', 'yes', 1, 'true', 'True', 'TRUE']


def write_to_excel(excel_data: dict, file_path, **kwargs):
    with pd.ExcelWriter(file_path, engine='openpyxl', engine_kwargs=None) as writer:
        for _sheet_name, _sheet_data in excel_data.items():
            _sheet_data.to_excel(writer, index=False, sheet_name=_sheet_name)

        writer.book.properties.creator = kwargs.get('author', '')
        writer.book.properties.title = kwargs.get('title', '')
        writer.book.properties.description = kwargs.get('description', '')
        writer.book.properties.subject = kwargs.get('subject', '')


def random_string(n=10):
    return ''.join(SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(n))


def get_closest(search, arr, index=False):
    closest, idx = None, None
    for i, item in enumerate(arr):
        if closest is None or abs(search - closest) > abs(item - search):
            closest = item
            idx = i
    if index:
        return closest, idx
    return closest


def validate_email(email):
    # pass the regular expression
    # and the string into the fullmatch() method
    if re.fullmatch(regex_email, email):
        return True
    else:
        return False


def validate_not_mobile(value):
    if regex_ph.search(value):
        msg = u"You cannot add mobile numbers."
        raise ValueError(msg)


def md5(data: str) -> str:
    if type(data) == str:
        data = data.encode()
    c = hashlib.md5(data)
    encrypt = c.hexdigest()
    return encrypt


def file_check(extracted_file):
    i = 0
    while i < 3:
        if os.path.isfile(extracted_file) and os.path.getsize(extracted_file) > 1000:
            break
        i += 1
        sleep(1)


def d_to_dt(d):
    if type(d) == datetime:
        return d  # Preserve object time and return
    if type(d) != date:
        return None
    return datetime.combine(d, datetime.min.time())


def socket_name(name):
    return f'ZWS_{name}'
