
#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='persist-stitch',
      version='0.3.1',
      description='A persister for the Stitch API',
      author='Stitch',
      url='https://github.com/stitchstreams/persist-stitch',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['persist_stitch'],
      install_requires=['stitchclient>=0.4.1',
                        'jsonschema',
                        'strict-rfc3339',
                        'stitchstream-python>=0.4.1'],
      entry_points='''
          [console_scripts]
          persist-stitch=persist_stitch:main
      '''
)
