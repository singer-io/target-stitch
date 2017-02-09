#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='target-stitch',
      version='0.5.2',
      description='A target for the Stitch API',
      author='Stitch',
      url='https://github.com/stitchstreams/target-stitch',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_stitch'],
      install_requires=['stitchclient>=0.4.1',
                        'jsonschema',
                        'strict-rfc3339',
                        'stitchstream-python>=0.4.1'],
      entry_points='''
          [console_scripts]
          target-stitch=target_stitch:main
      '''
)
