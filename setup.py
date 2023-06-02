# setup.py
from pathlib import Path

from setuptools import find_namespace_packages, setup

# Load packages from requirements.txt
BASE_DIR = Path(__file__).parent
with open(Path(BASE_DIR, "requirements.txt"), "r") as file:
    required_packages = [ln.strip() for ln in file.readlines()]


# setup.py
style_packages = ["black==22.3.0", "flake8==3.9.2", "isort==5.10.1"]

# setup.py
setup(
    name="ship_emissions_tracker",
    version=0.1,
    description="API service for maritime emissions data",
    python_requires=">=3.7",
    packages=find_namespace_packages(),
    install_requires=[required_packages],
    extras_require={"dev": style_packages + ["pre-commit==3.3.2"]},
)
