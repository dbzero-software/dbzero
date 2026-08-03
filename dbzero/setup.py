# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

from setuptools import setup
import sys

# Check for big-endian systems
if sys.byteorder == 'big':
    raise RuntimeError("dbzero does not support big-endian systems")

setup(
    name='dbzero',
    version='0.6.2',
    description='DISTIC (Durable, Infinite, Shared, Transactional, Isolated, Composable) storage system for Python 3.x offering flexibility of a memory with durability of a database.',
    packages=['dbzero'],
    python_requires='>=3.9',
    license='LGPL-2.1-or-later',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
