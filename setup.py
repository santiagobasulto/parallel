import re
import ast
import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()

VALUES = {
    '__version__': None,
    '__author__': None
}
with open('parallel/__init__.py', 'r') as f:
    tree = ast.parse(f.read())
    for node in tree.body:
        if node.__class__ != ast.Assign:
            continue
        target = node.targets[0]
        if hasattr(target, 'id') and target.id in VALUES:
            VALUES[target.id] = node.value.s


author_name, author_email = re.search(
    "(?P<name>.*) <(?P<email>.*)>", VALUES['__author__']).groups()

setuptools.setup(
    name="python-parallel",
    version=VALUES['__version__'],
    author=author_name,
    author_email=author_email,
    description="Simple parallelism for the everyday developer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/santiagobasulto/parallel",
    packages=['parallel'],
    python_requires=">=3.5",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
