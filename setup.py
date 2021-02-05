#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "oracle4dbt"
package_version = "0.0.1"
description = """The oracle adapter plugin for dbt (data build tool)"""
author = "rsella"
author_email = "-"
author_github = "github.com/rsella"

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author=author,
    author_email=author_email,
    url=author_github,
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/oracle/macros/*.sql',
            'include/oracle/dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt-core>=0.19.0",
        "cx_Oracle"
    ]
)
