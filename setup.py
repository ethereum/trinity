#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
from setuptools import setup, find_packages

PYEVM_DEPENDENCY = "py-evm==0.4.0a3"


deps = {
    'p2p': [
        "async-service==0.1.0a11",
        "asyncio-cancel-token>=0.2,<0.3",
        "async_lru>=0.1.0,<1.0.0",
        "cached-property>=1.5.1,<2",
        "coincurve>=15.0.0,<16.0.0",
        # cryptography does not use semver and allows breaking changes within `0.3` version bumps.
        "cryptography>=3.0,<3.2",
        "eth-enr>=0.3.0,<0.4",
        "eth-hash>=0.1.4,<1",
        "eth-keys>=0.3.3,<0.4.0",
        "eth-typing>=2.2.2,<3",
        "lru-dict>=1.1.6,<2",
        "pysha3>=1.0.0,<2.0.0",
        "python-snappy>=0.5.3",
        "rlp[rust-backend]>=2,<3",
        "SQLAlchemy>=1.3.3,<2",
        'trio>=0.16.0,<0.17',
        'trio-typing>=0.5.0,<0.6',
        "upnpclient>=0.0.8,<1",
    ],
    'trinity': [
        "aiohttp==3.6.0",
        "asks>=2.4.8,<3",
        "argcomplete>=1.12.2,<2",
        "asyncio-run-in-process==0.1.0a10",
        "bloom-filter==1.3",
        "cachetools>=3.1.0,<4.0.0",
        "eth-utils>=1.9.3,<2",
        "ipython>=7.8.0,<7.10.0",  # attach fails with v7.10.{0,1}
        "jsonschema>=3.2,<4",
        "lahja>=0.17.0,<0.18",
        "mypy-extensions>=0.4.3,<0.5.0",
        "plyvel==1.2.0",
        "prometheus-client==0.7.1",
        "psutil>=5.7.0, <6",
        PYEVM_DEPENDENCY,
        "pyformance==0.4",
        # requests 2.21 is required to support idna 2.8 which is required elsewhere
        "requests>=2.21,<3",
        "termcolor>=1.1.0,<2.0.0",
        "upnp-port-forward>=0.1.1,<0.2",
        "uvloop==0.14.0;platform_system=='Linux' or platform_system=='Darwin' or platform_system=='FreeBSD'",  # noqa: E501
        "web3>=5.12.1,<6",
        "websockets>=8.1.0",
    ],
    'test': [
        "async-timeout>=3.0.1,<4",
        "hypothesis>=4.45.1,<5",
        "pexpect>=4.6, <5",
        "factory-boy==2.12.0",
        "pytest>=5.3.0,<5.4",
        "pytest-cov>=2.11.1,<3",
        "pytest-mock>=1.12.1,<1.13",
        "pytest-randomly>=3.3.0,<4",
        "pytest-timeout>=1.4.2,<2",
        "pytest-watch>=4.2.0,<4.3",
        "pytest-xdist>=1.34.0,<2",
        "eth-tester==0.5.0b3",
    ],
    # We have to keep some separation between trio and asyncio based tests
    # because `pytest-asyncio` is greedy and tries to run all asyncio fixtures.
    # See: https://github.com/ethereum/trinity/pull/790
    # NOTE: In order to properly run any asyncio tests you need to manually install the
    # test-asyncio deps, otherwise pytest will run them but never await for them to finish and
    # you'll get warnings saying that a coroutine was never awaited.
    'test-asyncio': [
        "pytest-asyncio>=0.10.0,<0.11",
        "pytest-aiohttp>=0.3.0,<0.4",
    ],
    'test-trio': [
        "pytest-trio==0.6.0",
    ],
    'lint': [
        "flake8==3.7.9",
        "flake8-bugbear==19.8.0",
        "mypy==0.782",
        "sqlalchemy-stubs==0.3",
    ],
    'doc': [
        "pytest~=5.3",
        # Sphinx pined to `<1.8.0`: https://github.com/sphinx-doc/sphinx/issues/3494
        "Sphinx>=1.5.5,<1.8.0",
        "sphinx_rtd_theme>=0.1.9",
        "sphinxcontrib-asyncio>=0.2.0,<0.3",
        "towncrier>=19.2.0, <20",
    ],
    'dev': [
        "bumpversion>=0.5.3,<1",
        "wheel",
        "setuptools>=36.2.0",
        "tox==2.7.0",
        "twine",
    ],
}


def to_package_name(dependency):
    """
    Turn a dependency (e.g. "blspy>=0.1.8,<1") into the package name (e.g. "blspy")
    """
    return re.sub(r"[!=<>@ ](.|)+", "", dependency)


def filter_dependencies(package_list, *package_name):
    return list(filter(lambda x: to_package_name(x).lower() not in package_name, package_list))


# NOTE: Some dependencies break RTD builds. We can not install system dependencies on the
# RTD system so we have to exclude these dependencies when we are in an RTD environment.
if os.environ.get('READTHEDOCS', False):
    deps['p2p'] = filter_dependencies(deps['p2p'], 'python-snappy')

deps['dev'] = (
    deps['dev'] +
    deps['p2p'] +
    deps['trinity'] +
    deps['test'] +
    deps['doc'] +
    deps['lint']
)


install_requires = deps['trinity'] + deps['p2p']


with open('./README.md') as readme:
    long_description = readme.read()


setup(
    name='trinity',
    # *IMPORTANT*: Don't manually change the version here. Use the 'bumpversion' utility.
    version='0.1.0-alpha.37',
    description='The Trinity client for the Ethereum network',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Ethereum Foundation',
    author_email='piper@pipermerriam.com',
    url='https://github.com/ethereum/trinity',
    include_package_data=True,
    py_modules=['trinity', 'p2p'],
    python_requires=">=3.7,<4",
    install_requires=install_requires,
    extras_require=deps,
    license='MIT',
    zip_safe=False,
    keywords='ethereum blockchain evm trinity',
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    # trinity
    entry_points={
        'console_scripts': [
            'trinity=trinity:main',
        ],
    },
)
