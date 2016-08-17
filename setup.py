#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
import codecs
import os
import re

from setuptools import setup


def find_version(fpath):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, fpath), 'r') as f:
        version_file = f.read()
    matched = re.search(
        r"^__version__\s+=\s+['\"]([^'\"]*)['\"]", version_file, re.M)
    if matched:
        return matched.group(1)
    raise RuntimeError('version string undefined')


setup(
    name='pyrediq',
    description='Priority Queue with Redis',
    version=find_version('pyrediq/__init__.py'),
    packages=[
        'pyrediq'],
    scripts=[],
    url='https://github.com/okomestudio/pyrediq',
    install_requires=[
        'gevent',
        'msgpack-python',
        'redis'])
