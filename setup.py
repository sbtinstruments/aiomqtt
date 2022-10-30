# SPDX-License-Identifier: BSD-3-Clause
from setuptools import find_packages, setup

with open("asyncio_mqtt/version.py") as f:
    exec(f.read())

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup(
    name="asyncio_mqtt",
    version=__version__,  # type: ignore[name-defined]
    packages=find_packages(),
    package_data={
        "asyncio_mqtt": ["py.typed"],
    },
    url="https://github.com/sbtinstruments/asyncio-mqtt",
    author="Frederik Aalund",
    author_email="fpa@sbtinstruments.com",
    description="Idomatic asyncio wrapper around paho-mqtt.",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="BSD 3-clause License",
    license_files=("LICENSE",),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    keywords="mqtt async asyncio paho-mqtt wrapper",
    install_requires=[
        "paho-mqtt>=1.6.0",
        "async_generator;python_version<'3.7'",
        "typing_extensions;python_version<'3.10'",
    ],
    extras_require={
        "lint": ["mypy>=0.982", "flake8>=5.0.4", "types-paho-mqtt>=1.6.0.1"],
        "format": ["black>=22.10.0", "isort>=5.10.1"],
        "tests": ["pytest>=7.2.0", "pytest-cov>=4.0.0", "anyio>=3.6.2"],
    },
)
