#!/usr/bin/env python

from setuptools import setup

setup(name='target-stitch',
      version='0.6.1',
      description='Singer.io target for the Stitch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_stitch'],
      install_requires=[
          'stitchclient>=0.4.2',
          'jsonschema',
          'strict-rfc3339',
          'singer-python>=0.1.0',
      ],
      entry_points='''
          [console_scripts]
          target-stitch=target_stitch:main
      ''',
)
