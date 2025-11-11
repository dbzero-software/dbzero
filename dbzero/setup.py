from setuptools import setup
import sys

# Check for big-endian systems
if sys.byteorder == 'big':
    raise RuntimeError("dbzero does not support big-endian systems")

setup(
    name='dbzero',
    version='0.0.1',
    description='DBZero community edition',
    packages=['dbzero']
)
