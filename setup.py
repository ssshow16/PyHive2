import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='PyHive2',
    version='0.0.1',
    author='Bruce Shin',
    author_email='ssshow16@gmail.com',
    description='Python package for integration with Hive.',
    url='https://github.com/ssshow16/PyHive2',
    packages=['PyHive2'],
    package_data={'PyHive2': ['lib/*']},
    include_package_data=True,
    long_description=read('README.md'),
    )

