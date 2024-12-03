from setuptools import setup, find_packages

setup(name='dbxmetrics',
    version='0.1',
    author='Lucas Brabo',
    author_email='lucas.brabo@databricks.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        "databricks-connect",
        "databricks-sdk==0.34.0"
    ],
)