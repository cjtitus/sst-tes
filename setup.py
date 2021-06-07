#!/usr/bin/env python

"""The setup script."""

from setuptools import setup

setup(
    author="Charles Titus",
    author_email="charles.titus@nist.gov",
    install_requires=["bluesky",],
    name="sst_tes",
    entry_points={
        'databroker.handlers': [
            "tes = handlers.FakeHandler",
            ]
        },
    version="0.1.0",
)
