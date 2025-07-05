#!/usr/bin/env python3
"""
Setup script for PBS Monitor
"""

from setuptools import setup, find_packages
import os

# Read version from package
def read_version():
   """Read version from package __init__.py"""
   with open('pbs_monitor/__init__.py', 'r') as f:
      for line in f:
         if line.startswith('__version__'):
            return line.split('=')[1].strip().strip('"\'')
   return '0.1.0'

# Read long description from README
def read_long_description():
   """Read long description from README.md"""
   if os.path.exists('README.md'):
      with open('README.md', 'r', encoding='utf-8') as f:
         return f.read()
   return ''

# Read requirements from requirements.txt
def read_requirements():
   """Read requirements from requirements.txt"""
   requirements = []
   if os.path.exists('requirements.txt'):
      with open('requirements.txt', 'r') as f:
         for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
               requirements.append(line)
   return requirements

setup(
   name='pbs-monitor',
   version=read_version(),
   description='PBS scheduler monitoring and management tools',
   long_description=read_long_description(),
   long_description_content_type='text/markdown',
   author='PBS Monitor Team',
   author_email='contact@pbs-monitor.org',
   url='https://github.com/jtchilders/pbs_monitor',
   packages=find_packages(),
   python_requires='>=3.8',
   install_requires=read_requirements(),
   extras_require={
      'dev': [
         'pytest>=7.0.0',
         'pytest-cov>=4.0.0',
         'black>=22.0.0',
         'flake8>=5.0.0',
         'mypy>=0.991',
      ],
      'ml': [
         'torch>=1.13.0',
         'scikit-learn>=1.2.0',
         'jupyter>=1.0.0',
         'matplotlib>=3.5.0',
      ]
   },
   entry_points={
      'console_scripts': [
         'pbs-monitor=pbs_monitor.cli.main:main',
      ],
   },
   classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: System Administrators',
      'Intended Audience :: Science/Research',
      'License :: OSI Approved :: MIT License',
      'Operating System :: POSIX :: Linux',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.8',
      'Programming Language :: Python :: 3.9',
      'Programming Language :: Python :: 3.10',
      'Programming Language :: Python :: 3.11',
      'Topic :: System :: Monitoring',
      'Topic :: System :: Systems Administration',
      'Topic :: Scientific/Engineering',
   ],
   keywords='pbs scheduler monitoring hpc batch jobs',
   project_urls={
      'Bug Reports': 'https://github.com/jtchilders/pbs_monitor/issues',
      'Source': 'https://github.com/jtchilders/pbs_monitor',
      'Documentation': 'https://github.com/jtchilders/pbs_monitor',
   },
   include_package_data=True,
   zip_safe=False,
) 