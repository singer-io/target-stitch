#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='persist-stitch',
      version='0.2.3',
      description='A persister for the Stitch API',
      author='Stitch',
      url='https://github.com/stitchstreams/persist-stitch',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      packages=find_packages(),
      scripts=['bin/persist-stitch'],
      install_requires=['stitchclient>=0.4.1', 'jsonschema', 'strict-rfc3339'])
