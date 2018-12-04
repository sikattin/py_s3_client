# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='s3_client',
    version='1.0.0',
    description='client tool for managing s3 operation.',
    long_description=readme,
    author='takeki shikano',
    author_email='shikano.takeki@nexon.co.jp',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

