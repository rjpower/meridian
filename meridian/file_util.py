import gzip
import io
import json
import os
import arrow
from google.cloud import storage

CACHE_DIR = '/data/cache/'

client = storage.Client()


def cache_file(src_file):
    service, rest = src_file.split('://')
    bucket_name, blob_name = rest.split('/', 2)
    cache_filename = os.path.join(CACHE_DIR, rest.replace('/', '-'))

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if os.path.exists(cache_filename):
        cache_mtime = arrow.get(os.stat(cache_filename).st_mtime)
        blob_mtime = arrow.get(blob.time_created)
        if blob_mtime < cache_mtime:
            return cache_filename

    blob.download_to_filename(cache_filename)
    return cache_filename


def open_file(filename, mode='r', encoding=None):
    if filename.endswith('.gz'):
        return gzip.open(filename, mode, encoding=encoding)
    if filename.endswith('.bz2'):
        bz_file = os.popen('bzcat %s' % filename)
        if 'b' in mode:
            return io.BufferedReader(bz_file),
        else:
            return io.TextIOWrapper(io.BufferedReader(bz_file), encoding=encoding)
    if filename.startswith('gcloud://'):
        assert 'r' in mode
        cached_filename = cache_file(filename)
        return open_file(cached_filename)

    return io.open(filename, mode, encoding=encoding)


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
