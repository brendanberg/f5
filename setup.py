from setuptools import setup, find_packages
setup(
    name = "f5",
    version = "0.0.1",
    packages = find_packages(),
    download_url='https://github.com/vlaurenzano/f5/archive/v0.0.1.tar.gz',
    # metadata for upload to PyPI
    author = "Brendan Berg",
    author_email = "info@plusminusfive.com",
    description = "Use F5 to build more powerful Tornado apps",
    license = "MIT",
    keywords = "tornado orm rest api",
    url = "https://github.com/brendanberg/f5",
)
