from setuptools import setup
import sys

# Check for big-endian systems
if sys.byteorder == 'big':
    raise RuntimeError("dbzero does not support big-endian systems")

setup(
    name='dbzero',
    version='0.1.0',
    description='DBZero community edition',
    packages=['dbzero'],
    python_requires='>=3.8',
    license='AGPL-3.0-or-later',
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
