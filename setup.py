from setuptools import setup, find_packages

install_requires = [
    'requests>=2.2',
    'ckanapi==3.5',
    'unipath==1.1'
]

setup(
    author="CCG, Murdoch University",
    author_email="info@ccg.murdoch.edu.au",
    description="Ingest script for BPA data to CKAN",
    license="GPL3",
    keywords="",
    url="https://github.com/muccg/bpa-ingest",
    name="bpaingest",
    version="1.5.1",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'bpa-ingest=bpaingest.cli:main',
        ],
    }
)
