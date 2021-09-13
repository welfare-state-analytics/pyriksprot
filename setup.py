# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['pyriksprot', ] 

install_requires = \
['Jinja2>=2.11.3,<3.0.0',
 'click>=7.1.2,<8.0.0',
 'dehyphen>=0.3.4,<0.4.0',
 'untangle>=1.1.1,<2.0.0',
]

setup_kwargs = {
    'name': 'pyriksprot',
    'version': '2021.3.1',
    'description': 'Pipeline that transforms Parla-Clarin XML files',
    'long_description': '',
    'author': 'Roger Mähler',
    'author_email': 'roger.mahler@hotmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://westac.se',
    'packages': packages,
    'install_requires': install_requires,
    'python_requires': '==3.8.5',
}


setup(**setup_kwargs)
