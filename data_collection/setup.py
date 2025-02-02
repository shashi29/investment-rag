from setuptools import setup, find_packages

setup(
    name="data_collection",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'requests',
        'yfinance',
        'pandas',
        'numpy',
        'pydantic',
        'PyYAML',
        'aiohttp',
        'asyncio',
        'loguru',
        'cachetools',
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A system for collecting financial data from multiple sources",
    keywords="finance, data collection, api",
    url="http://github.com/yourusername/data_collection",
)
