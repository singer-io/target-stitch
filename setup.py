#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name="persist-stitch",
      version="0.1.0",
      description="Capture stitchstream data from stdin and persist it to the Stitch API",
      author="Stitch",
      url="https://github.com/stitchstreams/persist-stitch",
      packages=find_packages(),
      scripts=['bin/persist-stitch'],
      install_requires=["stitchclient", "transit-python"])
