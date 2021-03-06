from setuptools import (
    setup,
    find_packages,
)

setup(
    name='pylar',
    author='Julien Kauffmann',
    author_email='julien.kauffmann@freelan.org',
    maintainer='Julien Kauffmann',
    maintainer_email='julien.kauffmann@freelan.org',
    version=open('VERSION').read().strip(),
    url='http://ereOn.github.io/pylar',
    description=(
        "An experiment on micro-services.",
    ),
    long_description="""\
Pylar is an experiment on micro-services.
""",
    packages=find_packages(exclude=[
        'tests',
    ]),
    install_requires=[
        'aiohttp>=0.22.4,<1',
        'azmq[csodium]>=1.0.6,<2',
        'cachetools==1.1.6',
        'chromalog>=1.0.5,<2',
        'click>=6.6,<7',
        'csodium>=0.0.2',
        'entrypoints>=0.2.2',
        'pyslot>=2.0.1,<3',
    ],
    test_suite='tests',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Development Status :: 5 - Production/Stable',
    ],
    entry_points={
        'console_scripts': [
            'pylar-broker = pylar.entry_points:broker',
            'pylar-service = pylar.entry_points:service',
            'pylar-iservice = pylar.entry_points:iservice',
        ],
        'pylar_services': [
            'authentication = '
            'pylar.authentication_service:AuthenticationService',
            'arithmetic = pylar.arithmetic_service:ArithmeticService',
        ],
        'pylar_iservices': [
            'link = pylar.link_iservice:LinkIService',
        ],
    },
)
