#!/usr/bin/env python3
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ecs-s3-manager",
    version="0.1.0",
    author="Pictet STO",
    description="CLI tool for managing Dell EMC ECS S3 buckets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    py_modules=["s3cli"],
    include_package_data=True,
    install_requires=[
        "click>=8.1.3",
        "requests>=2.31.0",
        "pyyaml>=6.0",
        "boto3>=1.28.0",
    ],
    entry_points={
        "console_scripts": [
            "s3cli=s3cli:cli",
        ],
    },
    python_requires=">=3.7",
)