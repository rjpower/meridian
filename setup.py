#!/usr/bin/python

import setuptools

setuptools.setup(
  name='meridian',
  version='0.01',
  url='http://github.com/rjpower/meridian',
  install_requires=[
    'arrow',
    'google-cloud',
    'pandas',
    'numpy',
    'ipython',
    'tensorflow',
    'pytest',
    'requests',
    'six',
  ],
  packages=setuptools.find_packages(),
  package_dir={
    '' : '.'
  },
  zip_safe=False,
  entry_points='''
  '''
)
