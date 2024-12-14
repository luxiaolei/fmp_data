"""Setup configuration for fmp_data package."""

from setuptools import find_packages, setup

setup(
    name="fmp_data",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp>=3.8.0",
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "motor>=3.3.0,<4.0.0",
        "pymongo>=4.5.0,<5.0.0",
        "beanie>=1.21.0,<2.0.0",
        "exchange_calendars>=4.2.6",
        "pytz>=2023.3",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
        "loguru>=0.7.0",
        "click>=8.1.0",
    ],
) 