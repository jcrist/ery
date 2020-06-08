import os

from setuptools import setup

setup(
    name="ery",
    version="0.0.1",
    maintainer="Jim Crist-Harif",
    maintainer_email="jcristharif@gmail.com",
    license="BSD",
    packages=["ery"],
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    zip_safe=False,
)
