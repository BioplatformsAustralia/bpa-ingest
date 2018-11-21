from setuptools import setup, find_packages

setup(author="CCG, Murdoch University",
      author_email="info@ccg.murdoch.edu.au",
      description="Ingest script for BPA data to CKAN",
      license="GPL3",
      keywords="",
      url="https://github.com/muccg/bpa-ingest",
      name="bpaingest",
      version="5.2.1",
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      install_requires=[
            'unipath==1.1',
            'xlrd==1.0.0',
            'xlwt==1.2.0',
            'beautifulsoup4==4.6.0',
            'git+https://github.com/muccg/ckanapi.git@streaming-uploads',
            'requests==2.18.4',
            'httplib2==0.10.3',
            'boto3==1.4.7',
            'botocore==1.7.41',
            'google-api-python-client',
            'python-dateutil==2.6.1',
            'shapely==1.6.4',
            'fiona==1.7.13',
      ],
      entry_points={
          'console_scripts': [
              'bpa-ingest=bpaingest.cli:main',
          ],
      })
