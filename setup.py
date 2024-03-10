from setuptools import find_packages, setup
from typing import List

def get_requirements()->list:
    """
    This function will return a list of requiremnets
    """
    requirement_list:List[str] = []

    with open('requirements.txt', 'r') as f:
        required_packages = f.readlines()
        required_packages = [i.strip('\n') for i in required_packages]
        requirement_list.extend(required_packages)

    return requirement_list

get_requirements()

setup(
    name='finance-complaint',
    version='0.0.1',
    author='sarvjeet',
    author_email='sjitbh121993@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)