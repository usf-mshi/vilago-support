import setuptools

setuptools.setup(
    url='https://github.com/andrew-nguyen/vilago-support',
    author='Andrew Nguyen',
    author_email='andrew@na-consulting.net',
    name='vilago-support',
    version='0.0.1-SNAPSHOT',
    install_requires=['pyedflib', 'google-cloud-storage'],
    packages=setuptools.find_packages(),
)
