#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="hadoop_mock",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            'hadoop_mock = hadoop_mock.hadoop_mock:cli',
            'hadoop_ssh = hadoop_mock.hadoop_ssh:cli',
        ]
    },
    author="Andrei Fyodorov",
    author_email="sour-times@yandex.ru",
    description="",
    license="PSF",
    url="https://github.com/andreifyodorov/hadoop_mock"
)
