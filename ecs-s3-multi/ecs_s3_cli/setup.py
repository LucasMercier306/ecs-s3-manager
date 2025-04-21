from setuptools import setup, find_packages

setup(
    name='ecs_s3_cli',
    version='0.1.0',
    packages=find_packages(),
    install_requires=['click','PyYAML','tabulate','ecs_s3_client'],
    #install_requires=['click','PyYAML','tabulate'],
    entry_points={
        'console_scripts': ['ecs-s3=ecs_s3_cli.commands:cli']
    }
)
