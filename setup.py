import os

from setuptools import setup, find_packages


requires = [
    'pymysql',
    'pyodbc',
    'pudb',
    'configparser',
    'cryptography'
    ]

setup(name='tnt_taxamerger',
      version='0.1',
      description='Merge taxonomies from different DiversityTaxonNames databases into one MySQL database',
      author='Björn Quast',
      author_email='bquast@leibniz-zfmk.de',
      install_requires=requires
      )
