from setuptools import setup, find_packages

setup(
    name="convertur",
    version="0.1.3",
    packages=find_packages(),
    install_requires=[
        'boto3',
        'prompt_toolkit',
        'pygments',
        'requests',
        'questionary',
        'pandas',
        'pyarrow', # for parquet
        "click>=8.0.0",
    ],
    author="Iakov Gan, Steph Gooch, Zach Erdman",
    entry_points={
        'console_scripts': [
            'migrate-cur1=convertur.migrate:main',
            'query-converter=convertur.query_converter:main',
        ],
    },
    author_email="iakov@amazon.com",
    description="ConvertUR for CUR",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/aws-samples/aws-data-exports-setup",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)