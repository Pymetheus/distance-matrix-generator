from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="distance_matrix",
    version="1.0.2",
    description="A modular pipeline to generate real-world distance matrixes using the Google Maps API and store them in SQL databases.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Pymetheus",
    author_email="github.senate902@passfwd.com",
    url="https://github.com/Pymetheus/distance-matrix-generator",
    license="MIT",
    packages=find_packages(),
    include_package_data=False,
    install_requires=[
        'googlemaps>=4.10.0',
        'sqlalchemy>=2.0',
        'sqlalchemy-dbtoolkit>=0.1.7',
        'pandas>=2.2.0',
        'numpy>=2.2.0',
        'psycopg2>=2.9.0',
        'mysql-connector-python>=9.3.0'
    ],
    python_requires=">=3.8"
)
