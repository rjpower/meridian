#!/usr/bin/env python

import collections
import gzip
import io
import json
import logging
import os
import time
import types

import arrow
import numpy as np

CACHE_DIR = '/data/'


class RateLimiter(object):
    def __init__(self, limit=1, interval=1):
        self._limit = limit
        self._interval = interval
        self._count = 0
        self._last_polled = 0

    def wait(self):
        delta = time.time() - self._last_polled
        if delta > self._interval:
            self._count = 0
            self._last_polled = time.time()

        self._count += 1
        if self._count > self._limit:
            logging.info('Exceeded limit, sleeping %s', self._interval - delta)
            time.sleep(self._interval - delta)


def flatten(lst):
    if isinstance(lst, (list, types.GeneratorType)):
        result = []
        for v in lst:
            if not isinstance(v, list):
                result.append(v)
            else:
                result.extend(flatten(v))
        return result
    return lst


def cached_op(target_file):
    def _fn(*args, **kw):
        if os.path.exists(target_file):
            return target_file

        with open(target_file + '.tmp', 'w') as f:
            _fn(f, *args, **kw)

        os.rename(target_file + '.tmp', target_file)

    return _fn


def batchify_dict(generator, batch_size=64):
    batch_dict = collections.defaultdict(list)
    count = 0
    for row in generator:
        for k, v in row.items():
            batch_dict[k].append(v)

        count += 1
        if count >= batch_size:
            yield {
                k: np.array(v) for (k, v) in batch_dict.items()
            }
            count = 0
            batch_dict = {}
