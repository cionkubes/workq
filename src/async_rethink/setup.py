from setuptools import setup, find_packages
setup(
    name="async_rethink",
    version="0.1",
    license="GPLv3",
    install_requires=[
        'rx',
    ],
    packages=find_packages(),
)