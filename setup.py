from setuptools import setup, find_packages

setup(author="CCG, Murdoch University",
      author_email="help@bioplatforms.com",
      description="Ingest script for BPA data to CKAN",
      license="GPL3",
      keywords="",
      url="https://github.com/BioplatformsAustralia/bpa-ingest",
      name="bpaingest",
      version="6.2.2",
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      entry_points={
          'console_scripts': [
              'bpa-ingest=bpaingest.cli:main',
          ],
      })
