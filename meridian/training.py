import collections
import numpy as np


def batchify(generator, batch_size):
  batch_items = collections.defaultdict(list)
  for sample in generator:
    for k, v in sample.items():
      batch_items[k].append(v)

    if len(batch_items[k]) >= batch_size:
      yield {k: np.asarray(v) for (k, v) in batch_items.items()}
      for k in batch_items.keys():
        del batch_items[k][:]


def forever(generator_fn):
  while True:
    for result in generator_fn():
      yield result
