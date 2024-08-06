from setuptools import find_packages, setup

setup(
    name="transformation",
    packages=find_packages(exclude=["transformation_tests"]),
    install_requires=[
        "dagster==1.7.15",
        "dagster-cloud==1.7.15",
        "dagster-databricks==0.23.15",
    ],
    extras_require={"dev": ["dagster-webserver==1.7.15", "pytest"]},
)
