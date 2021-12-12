#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='labkeyext-dasievers',
    version='0.1.0',
    description='Extensions to Python scripts running on LabKey servers.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/dasievers/labkeyext',
    author='David Sievers',
    packages=['labkeyext'],
    install_requires=[
                      'pandas',
                      ],

    classifiers=[
                'Development Status :: 3 - Alpha',
                'License :: OSI Approved :: Apache Software License',
                'Intended Audience :: Science/Research',
                'Topic :: Database',
                'Programming Language :: Python :: 3',
                ],
    python_requires=">=3.6",
)
