"""
simple downloader for saving a whole ElasticSearch-index, just the corresponding type or just a document into a line-delimited JSON-File
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='es2json',
      version='0.2.0',
      description='simple downloader for saving a whole ElasticSearch-index, just the corresponding type or just a document into a line-delimited JSON-File',
      url='https://github.com/slub/es2json',
      author='Bernhard Hering',
      author_email='bernhard.hering@slub-dresden.de',
      license="Apache 2.0",
      packages=['es2json', 'helperscripts', 'oldapi_calls', 'cli'],
      package_dir={'es2json': 'es2json',
                   'helperscripts': 'es2json',
                   'oldapi_calls': 'es2json',
                   'cli': 'es2json'},
      install_requires=[
          'argparse>=1.4.0',
          'elasticsearch>=6.0.0',
          'elasticsearch_dsl>=6.0.0',
          'httplib2>=0.17.0'
      ],
      python_requires=">=3.5,<4",
      entry_points={
          "console_scripts": ["es2json=es2json.cli:run"]
          }
      )
