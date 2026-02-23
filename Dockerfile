FROM python:3.14-alpine AS base

# Install some useful utilities in image
RUN apk add tzdata bash vim git
RUN python -m pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --upgrade build

# Set TZ to East Coast
ARG TZ="America/New_York"
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime

# Work in tmp, pull the repo and build it here
WORKDIR /tmp
ARG CACHE_BUST
RUN git clone https://github.com/gordonaspin/icloudpd.git
WORKDIR /tmp/icloudds
RUN python -m build

# Install the wheel, check commands work
RUN pip install dist/*.whl
RUN pip list -v
RUN icloud -h
RUN icloudpd -h

# Add the docker user
ARG GROUP_NAME=docker
ARG USER_NAME=docker
ARG USER_UID=1000
ARG GROUP_GID=1000
# Create a group and a user, then add the user to the group
RUN addgroup -g ${GROUP_GID} -S ${GROUP_NAME} && \
    adduser -u ${USER_UID} -S -G ${GROUP_NAME} -D -H ${USER_NAME}

# docker home, copy base config files and chown them to docker
WORKDIR /home/docker
RUN rm -rf /tmp/icloudds

# Set the TZ, change user to docker, define entrypoint
ENV TZ=${TZ}
USER docker
ENTRYPOINT [ "icloudpd", "-d", "/drive", "--cookie-directory", "/cookies" ]

