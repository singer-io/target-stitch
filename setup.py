#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name="persist-stitch",
      version="0.1.0",
      description="A persister for the Stitch API",
      author="Stitch",
      url="https://github.com/stitchstreams/persist-stitch",
      packages=find_packages(),
      scripts=['bin/persist-stitch'],
      install_requires=["stitchclient"])
