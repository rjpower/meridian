import gzip
import io
import json
import logging
import os
import tempfile

import arrow
import numpy
from google.cloud import storage

CACHE_DIR = '/data/cache/'

_client = None

def client():
    global _client
    if not _client:
        _client = storage.Client()
    return _client


def filename_to_blob(src_file, create_if_missing=False):
    service, rest = src_file.split('://')
    bucket_name, blob_name = rest.split('/', 1)
    bucket = client().get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    if blob is not None:
        return blob

    if create_if_missing:
        return bucket.blob(blob_name)

    return None


def cache_file(src_file):
    if not '://' in src_file:
        return src_file

    logging.info('Caching: %s', src_file)
    service, rest = src_file.split('://')
    bucket_name, blob_name = rest.split('/', 1)
    cache_filename = os.path.join(CACHE_DIR, rest.replace('/', '-'))

    if not os.path.exists(CACHE_DIR):
        os.system('mkdir -p "%s"' % CACHE_DIR)

    blob = filename_to_blob(src_file)

    if not blob:
        raise FileNotFoundError(src_file)

    if os.path.exists(cache_filename):
        cache_mtime = arrow.get(os.stat(cache_filename).st_mtime)
        blob_mtime = arrow.get(blob.time_created)
        logging.info('Cache exists %s (%s, %s)', src_file, cache_mtime, blob_mtime)
        if blob_mtime < cache_mtime:
            logging.info('Cache file exists and is up to date: %s' % src_file)
            return cache_filename

    blob.download_to_filename(cache_filename + '.tmp')
    os.rename(cache_filename + '.tmp', cache_filename)
    return cache_filename


def open_file(filename, mode='r', encoding=None):
    if filename.startswith('gcloud://'):
        assert 'r' in mode
        cached_filename = cache_file(filename)
        return open_file(cached_filename, mode, encoding)

    if filename.endswith('.gz'):
        return gzip.open(filename, mode, encoding=encoding)
    if filename.endswith('.bz2'):
        bz_file = os.popen('bzcat %s' % filename)
        if 'b' in mode:
            return io.BufferedReader(bz_file),
        else:
            return io.TextIOWrapper(io.BufferedReader(bz_file), encoding=encoding)

    return io.open(filename, mode, encoding=encoding)


class CachedContext(object):
    def __init__(self, filename, mode, encoding):
        self._filename = filename
        self._mode = mode
        self._encoding = encoding

    def __enter__(self):
        self._temp = tempfile.NamedTemporaryFile(mode=self._mode, encoding=self._encoding)
        return self._temp

    def __exit__(self, x, y, z):
        logging.info('Uploading %s (%s MB)',
                     self._filename, os.stat(self._temp.name).st_size / 1048576)
        blob = filename_to_blob(self._filename, create_if_missing=True)
        self._temp.flush()
        blob.upload_from_filename(self._temp.name)


def open_ctx(filename, mode='rb', encoding=None):
    """
    Provide a context manager for working with a file object.

    Limiting usage to the context manager scope simplifies handling
    remote objects.
    """
    if not '://' in filename:
        return open_file(filename, mode, encoding)

    if 'r' in mode:
        return open_file(cache_file(filename), mode, encoding)

    return CachedContext(filename, mode, encoding)


def read_json_lines(filename):
    return [json.loads(l) for l in open_file(filename, mode='rt').readlines()]


def write_json_lines(ary, filename):
    with open_file(filename, 'w') as f:
        for item in ary:
            json.dump(item, f)
            f.write('\n')


def write_json(filename, obj):
    with open_file(filename, 'w') as f:
        json.dump(obj, f)


def read_json(filename):
    with open_file(filename, 'r') as f:
        return json.load(f)

def read_numpy(filename):
    return numpy.load(cache_file(filename))

def write_numpy(filename, obj):
    with open_ctx(filename, 'wb') as f:
        return numpy.save(f, obj)
