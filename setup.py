# -*- coding: utf-8 -*-


from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='s3_client',
    version='1.0.2',
    description='client tool for managing s3 operation.',
    long_description=readme,
    author='takeki shikano',
    author_email='',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

