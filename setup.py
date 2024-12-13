"""Setup configuration for FMP data package."""

from setuptools import find_packages, setup

setup(
    name="fmp_data",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=3.8.0",
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "motor>=3.3.0",
        "pymongo>=4.5.0",
        "exchange_calendars>=4.2.6",
        "pytz>=2023.3",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
        "loguru>=0.7.0",
        "click>=8.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "isort>=5.12.0",
            "mypy>=1.5.0",
            "flake8>=6.1.0",
        ]
    },
    python_requires=">=3.8",
) 