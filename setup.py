from setuptools import setup, find_packages
setup(
    name = "f5",
    version = "0.0.3",
    packages = find_packages(),
    download_url='https://github.com/brendanberg/f5/archive/v0.0.2.tar.gz',
    # metadata for upload to PyPI
    author = "Brendan Berg",
    author_email = "brendan@berg.industries",
    description = "Use F5 to build more powerful Tornado apps",
    license = "MIT",
    install_requires = [
        'msgpack',
    ],
    keywords = "tornado orm rest api",
    url = "https://github.com/brendanberg/f5",
)
