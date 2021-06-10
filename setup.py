#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

setup(
    author="Charles Titus",
    author_email="charles.titus@nist.gov",
    install_requires=["bluesky",],
    name="sst_tes",
    entry_points={
        'databroker.handlers': [
            "tes = sst_tes.handlers:SimpleHandler",
            "tessim = sst_tes.handlers:FakeHandler",
            ]
        },
    version="0.1.0",
    packages=find_packages()
)
