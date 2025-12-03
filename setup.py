from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="icloudpd",
    version="2.1.1",
    url="https://github.com/gordonaspin/icloud_photos_downloader",
    description=(
        "icloudpd is a command-line tool to download photos and videos from iCloud."
    ),
    maintainer="Gordon Aspin",
    maintainer_email="gordon.aspin@gmail.com",
    license="MIT",
    packages=find_packages(),
    install_requires=required,
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={"console_scripts": ["icloudpd = icloudpd.base:main"]},
)
