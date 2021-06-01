with open('requirements.txt') as f:
    requirements = f.read().splitlines()

...

setup(
    name='theorize',
    version='0.0.1',
    install_requires=requirements,
)
