#!/usr/bin/env python
"""
Setup script for self-healing data pipeline backend.

This script defines package metadata, dependencies, entry points, and installation
requirements for the backend components of the self-healing data pipeline.
"""

import os
import io
import setuptools
from constants import *  # Import constants module for package configuration

def read(filename):
    """
    Reads the content of a file relative to the setup.py location.
    
    Args:
        filename (str): Name of the file to read
        
    Returns:
        str: Content of the file
    """
    here = os.path.abspath(os.path.dirname(__file__))
    filepath = os.path.join(here, filename)
    with io.open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def get_requirements():
    """
    Parses requirements.txt and returns a list of dependencies.
    
    Returns:
        list: List of package requirements
    """
    content = read('requirements.txt')
    requirements = [
        line.strip() for line in content.split('\n')
        if line.strip() and not line.startswith('#')
    ]
    return requirements

setuptools.setup(
    name='self-healing-pipeline',
    version='0.1.0',
    description='An end-to-end self-healing data pipeline for BigQuery using Google Cloud services and AI-driven automation',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Data Engineering Team',
    author_email='data-engineering@example.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Database Engines/Servers',
    ],
    keywords='data pipeline, self-healing, bigquery, google cloud, data quality, monitoring',
    packages=setuptools.find_packages(),
    python_requires='>=3.9',
    install_requires=get_requirements(),
    entry_points={
        'console_scripts': [
            'pipeline-cli=src.backend.cli:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    project_urls={
        'Documentation': 'https://github.com/example/self-healing-pipeline/docs',
        'Source': 'https://github.com/example/self-healing-pipeline',
        'Issues': 'https://github.com/example/self-healing-pipeline/issues',
    },
)