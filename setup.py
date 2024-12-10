from setuptools import setup, find_packages

setup(
    name="bson-tools",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pymongo>=3.11.0",
        "PyYAML>=5.4.1",
    ],
    entry_points={
        "console_scripts": [
            "bson-tools=bson_tools.cli:main",
        ],
    },
    author="Jannik Janket",
    author_email="jannikjanket@gmail.com",
    description="A comprehensive toolkit for BSON file manipulation",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/janketj/bson-tools",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
