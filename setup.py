from setuptools import setup, find_packages

version = '0.1'

setup(
      name = "python-hiveserver2",
      version = version,
      url = '',
      description = "Python HiveServer 2 Client Binding",
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      install_requires = ['setuptools'],
      include_package_data=True,
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
