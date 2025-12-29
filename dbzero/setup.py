# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from setuptools import setup
import sys

# Check for big-endian systems
if sys.byteorder == 'big':
    raise RuntimeError("dbzero does not support big-endian systems")

setup(
    name='dbzero',
    version='0.1.2',
    description='DBZero community edition',
    packages=['dbzero'],
    python_requires='>=3.9',
    license='LGPL-2.1-or-later',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
