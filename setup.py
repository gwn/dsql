from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dsql',
    version='0.3.2',
    description='Dead simple RDBMS handling lib',
    long_description=long_description,
    url='https://github.com/gwn/dsql',
    author='Ege Avunc',
    author_email='egeavunc@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='sql db query builder simple',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
)
