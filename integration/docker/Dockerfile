#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

# Use a multi-stage build to include artifacts without using a chown
# chown in bash nearly doubles the image size of the docker image.
# See:
# - https://stackoverflow.com/questions/30085621/why-does-chown-increase-size-of-docker-image
# - https://github.com/moby/moby/issues/5505
# - https://github.com/moby/moby/issues/6119
FROM alpine:3.10.2 AS extractor
ARG ALLUXIO_TARBALL=http://downloads.alluxio.io/downloads/files/2.1.0/alluxio-2.1.0-bin.tar.gz

ADD ${ALLUXIO_TARBALL} /opt/
# if the tarball was remote, it needs to be untarred
# use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

# FROM openjdk:8-jdk-alpine
FROM alpine:3.9

# A few reasons for installing distribution-provided OpenJDK:
# https://hub.docker.com/layers/openjdk/library/openjdk/8-jdk-alpine3.9/images/sha256-210ecd2595991799526a62a7099718b149e3bbefdb49764cc2a450048e0dd4c0

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
    echo '#!/bin/sh'; \
    echo 'set -e'; \
    echo; \
    echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
  } > /usr/local/bin/docker-java-home \
  && chmod +x /usr/local/bin/docker-java-home
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk
ENV PATH $PATH:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin

ENV JAVA_VERSION 8u222
ENV JAVA_ALPINE_VERSION 8.222.10-r0

RUN set -x \
  && apk add --no-cache \
    openjdk8="$JAVA_ALPINE_VERSION" \
    openjdk8-dbg="$JAVA_ALPINE_VERSION" \
    bash \
    su-exec \
  && [ "$JAVA_HOME" = "$(docker-java-home)" ]

ARG ALLUXIO_USERNAME=alluxio
ARG ALLUXIO_GROUP=alluxio
ARG ALLUXIO_UID=1000
ARG ALLUXIO_GID=1000

RUN apk --no-cache --update add bash libc6-compat shadow && \
    rm -rf /var/cache/apk/*

# disable JVM DNS cache
RUN echo "networkaddress.cache.ttl=0" >> $JAVA_HOME/jre/lib/security/java.security

# add the following for native libraries needed by rocksdb
ENV LD_LIBRARY_PATH /lib64:${LD_LIBRARY_PATH}

# if Alluxio user, group, gid, and uid aren't root|0
# then create the alluxio user and set file permissions accordingly
RUN if [ ${ALLUXIO_USERNAME} != "root" ] \
    && [ ${ALLUXIO_GROUP} != "root" ] \
    && [ ${ALLUXIO_UID} -ne 0 ] \
    && [ ${ALLUXIO_GID} -ne 0 ]; then \
      addgroup -g ${ALLUXIO_GID} -S ${ALLUXIO_GROUP} && \
      adduser -u ${ALLUXIO_UID} -S ${ALLUXIO_USERNAME} -G ${ALLUXIO_GROUP} && \
      usermod -a -G root ${ALLUXIO_USERNAME} && \
      mkdir -p /journal && \
      chown -R ${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} /journal && \
      chmod -R g=u /journal; \
    fi

# Docker 19.03+ required to expand variables in --chown argument
# https://github.com/moby/buildkit/pull/926#issuecomment-503943557
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} --from=extractor /opt/ /opt/
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} conf /opt/alluxio/conf/
COPY --chown=${ALLUXIO_USERNAME}:${ALLUXIO_GROUP} entrypoint.sh /

USER ${ALLUXIO_UID}

WORKDIR /opt/alluxio

ENV PATH="/opt/alluxio/bin:${PATH}"

ENTRYPOINT ["/entrypoint.sh"]
