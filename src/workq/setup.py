from setuptools import setup, find_packages

setup(
    name="workq",
    version="0.1",
    license="GPLv3",
    packages=find_packages(),
    install_requires=[
        'logzero',
    ],
)