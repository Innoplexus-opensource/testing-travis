try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools

    use_setuptools()
    from setuptools import setup

import os
import sys


test_suite = "tests"
tests_require = ["unittest"]

if sys.version_info[:2] == (2, 6):
    # Need unittest2 to run unittests in Python 2.6
    tests_require.append("unittest2")
    test_suite = "unittest2.collector"

try:
    with open("README.rst", "r") as fd:
        long_description = fd.read()
except IOError:
    long_description = None  # Install without README.rst


setup(
    name='arango3-doc-manager',
    version='0.1.4',
    maintainer='Innoplexus',
    description='Arango3 plugin for mongo-connector',
    long_description=long_description,
    platforms=['any'],
    author='Prashant Patil',
    author_email='prashant.patil@innoplexus.com',
    url='https://github.com/Innoplexus-Consulting-Services/arango3-doc-manager.git',
    install_requires=[
        'mongo-connector>=2.5.0',
        'python-arango>=3.12.1'],
    packages=[
        "mongo_connector",
        "mongo_connector.doc_managers"],
    license="MIT License",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
    ],
    keywords=[
        'mongo-connector',
        "mongodb",
        "arango",
        "arangodb"],
    scripts=['bin/connector_arango_auth'],
    test_suite=test_suite,
    tests_require=tests_require)
