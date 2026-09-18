from setuptools import setup

setup(
    name='dataxsl',
    version='1.1.1',
    packages=['dataxsl', 'dataxsl.excel', 'dataxsl.mysql', 'dataxsl.oracle', 'dataxsl.postgresql'],
    package_dir={'': 'src'},
    python_requires='>=3.12',
    install_requires=[
        'jsonschema>=4.18,<5', 'PyYAML>=6,<7', 'pandas>=2.0,<3',
        'python-calamine>=0.5,<1', 'msoffcrypto-tool>=5,<6',
        'PyMySQL>=1.1,<2', 'psycopg[binary]>=3.1,<4', 'pycryptodome>=3.18,<4',
    ],
    license='Apache-2.0',
    author='carlostevez',
    author_email='tevez0014@163.com',
    description='Single-pipeline offline data conversion',
)
