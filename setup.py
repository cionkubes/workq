from setuptools import setup, find_packages

setup(
    name="workq",
    version="1.0.3",
    license="GPLv3",
    packages=find_packages(),
    install_requires=[
        'logzero',
        'rx'
    ],
)
