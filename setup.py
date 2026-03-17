"""Setup script for Daily Databricks Feed."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="daily-databricks-feed",
    version="0.1.0",
    author="Daily Databricks Feed Team",
    author_email="podcast@example.com",
    description="A daily podcast generator for Databricks and data engineering news",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/daily-databricks-feed",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Multimedia :: Sound/Audio :: Speech",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content :: News/Diary",
    ],
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "praw>=7.7.0",
        "feedparser>=6.0.0",
    ],
    extras_require={
        "gcp": [
            "google-auth>=2.22.0",
            "google-auth-oauthlib>=1.0.0",
            "google-cloud-storage>=2.10.0",
            "google-cloud-texttospeech>=2.14.0",
        ],
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
        ],
    },
    entry_points={},
)
