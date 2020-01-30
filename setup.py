"""
simple downloader for saving a whole ElasticSearch-index, just the corresponding type or just a document into a line-delimited JSON-File
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='es2json',
      version='0.0.1',
      description='simple downloader for saving a whole ElasticSearch-index, just the corresponding type or just a document into a line-delimited JSON-File',
      url='https://github.com/slub/es2json',
      author='Bernhard Hering',
      author_email='bernhard.hering@slub-dresden.de',
      license="Apache 2.0",
      packages=['es2json'],
      package_dir={'es2json': 'es2json'},
      install_requires=[
          'argparse>=1.4.0',
          'elasticsearch>=5.0.0'
      ],
      python_requires=">=3.6.*",
      entry_points={
          "console_scripts": ["es2json=es2json.es2json:run"]
          }
      )
