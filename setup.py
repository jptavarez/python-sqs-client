try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = "0.0.2"
setup(
    name="sqs-client",
    version=version,
    author="João Paulo Tavares da Silva",
    author_email="jptavarez.silva@gmail.com",
    description=(""),
    license="BSD",
    packages=["sqs_client"],
    install_requires=['boto3', 'multiprocessing-logging'],
)