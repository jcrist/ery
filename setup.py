import os

from setuptools import setup, find_packages
from setuptools.extension import Extension

ext_modules = [Extension("ery.core._lib", ["ery/core/_lib.c"])]

setup(
    name="ery",
    version="0.0.1",
    maintainer="Jim Crist-Harif",
    maintainer_email="jcristharif@gmail.com",
    license="BSD",
    packages=find_packages(),
    ext_modules=ext_modules,
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    zip_safe=False,
)
