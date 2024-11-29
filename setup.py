#!/usr/bin/env python

from setuptools import setup

setup(name='target-stitch',
      version='4.0.0',
      description='Singer.io target for the Stitch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_stitch'],
      install_requires=[
          'jsonschema==2.6.0',
          'mock==2.0.0',
          'requests==2.31.0',
          'singer-python==6.0.0',
          'psutil==5.6.6',
          'simplejson==3.11.1',
          'aiohttp==3.10.11',
	  'ciso8601',
      ],
      extras_require={
          'dev': [
              'nose==1.3.7',
              'astroid==3.2.4',
              'pylint==3.2.7'
          ]
      },
      entry_points='''
          [console_scripts]
          target-stitch=target_stitch:main
      ''',
      packages=['target_stitch'],
)
