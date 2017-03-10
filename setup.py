# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('bmetrica/version.py') as f:
    exec(f.read())

setup(
    name='bmetrica',
    version=__version__,
    description="bmetrica: A CLI interface to IBM's Platform LSF MySQL/Cacti database",
    long_description=readme,
    author="Indraniel Das",
    author_email="indraniel@gmail.com",
    license=license,
    url='https://github.com/indraniel/bmetrica',
    dependency_links=[],
    install_requires=[
        'Jinja2>=2.4',
        'click>=6.7',
        'PyMySQL>=0.7.10'
    ],
    entry_points='''
        [console_scripts]
        bmetrica=bmetrica.cli:cli
    ''',
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
)
