from __future__ import print_function
from setuptools import setup, find_packages
import sys

setup(
    name="redis_ts",
    version="0.1.0",
    author="onebula",
    author_email="",
    description="Simple python wrapper for time series data in redis.",
    license="MIT",
    url="https://github.com/onebula/redis_ts",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Environment :: Web Environment",
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: Chinese',
        'Operating System :: MacOS',
        'Operating System :: Microsoft',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Topic :: NLP',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=[
            'ciso8601>=2.0.0',  #所需要包的版本号
            'redis>=3.5.0'   #所需要包的版本号
    ],
    zip_safe=True,
)