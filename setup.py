#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "oracle4dbt"
package_version = "0.0.2"
description = """The oracle adapter plugin for dbt (data build tool)"""
author = "rsella"
author_email = "rsella94@gmail.com"
github = "https://github.com/rsella/oracle4dbt"

with open('README.md', 'r', encoding='UTF-8') as f:
    long_description = f.read()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=author,
    author_email=author_email,
    url=github,
    packages=find_packages('.', exclude=['test']),
    package_data={
        'dbt': [
            'include/oracle/macros/*.sql',
            'include/oracle/dbt_project.yml',
            'include/oracle/sample_profiles.yml'
        ]
    },
    install_requires=[
        "dbt-core>=0.19.0",
        "cx_Oracle"
    ],
    python_requires='>=3.5'
)
