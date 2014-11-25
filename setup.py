# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from setuptools import setup

setup(
    name='elasticdata',
    version='0.0.1-pre',
    url='http://karolsikora.me/elasticdata',
    author='Karol Sikora',
    author_email='me@karolsikora.me',
    description='A high level framework to manage data stored in elasticsearch.',
    license='BSD',
    packages=['elasticdata'],
    install_requires=[
        'Django>=1.6.7,<1.7',
        'elasticsearch>=1.0.0,<2.0.0',
        'six',
        'python-dateutil',
        'inflection'
    ],
    test_requires=['mock'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite='tests'
)
