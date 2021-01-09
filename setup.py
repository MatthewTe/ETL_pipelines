from setuptools import setup


here = path.abspath(path.dirname(__file__))

setup(
    name = "ETL Pipeline API",
    url = "https://github.com/MatthewTe/ETL_pipelines",
    author = "Matthew Teelucksingh",
    packages = setuptools.find_packages(),
    license = 'MIT',
    long_description=open('README.md').read()   
    )