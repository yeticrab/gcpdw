# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='sample',
    version='0.1.0',
    description='sample_package',
    author='Roger Gill',
    author_email='roger.gill@travelsupermarket.com',
    packages=find_packages(exclude=('tests')),
	test_suite='nose.collector',
    tests_require=['nose']
)

