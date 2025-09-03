FROM ubuntu:24.04 AS tarantool-base

ARG TARANTOOL_FULL_VERSION="2.11.7.g4e040601502-1" \
    GC64=ON \
    ROCKS_INSTALLER="tarantoolctl rocks"

ENV GC64=${GC64:-OFF} \
    TARANTOOL_FULL_VERSION=${TARANTOOL_FULL_VERSION} \
    LUAROCK_VSHARD_VERSION=0.1.26 \
    LUAROCK_AVRO_SCHEMA_VERSION=3.1.0 \
    LUAROCK_EXPERATIOND_VERSION=1.6.0 \
    LUAROCK_QUEUE_VERSION=1.3.3 \
    LUAROCK_CONNPOOL_VERSION=1.1.1 \
    LUAROCK_HTTP_VERSION=1.5.0 \
    LUAROCK_MEMCACHED_VERSION=1.1.1 \
    LUAROCK_METRICS_VERSION=1.0.0 \
    LUAROCK_TARANTOOL_PG_VERSION=2.0.2 \
    LUAROCK_TARANTOOL_MYSQL_VERSION=2.1.3 \
    LUAROCK_TARANTOOL_GIS_VERSION=1.0.1 \
    LUAROCK_TARANTOOL_GPERFTOOLS_VERSION=1.0.1

RUN set -x \
    && useradd --user-group --create-home --no-log-init --shell /bin/bash tarantool \
    && apt update && apt -y --no-install-recommends --no-install-suggests install \
    libgoogle-perftools4 \
    lsb-release \
    unzip \
    luarocks \
    libgeos-dev \
    libsasl2-dev \
    libssl-dev \
    libpq-dev \
    gis-devel \
    libproj-dev \
    gosu \
    curl \
    wget \
    gnupg2 \
    ca-certificates \
    apt-transport-https \
    && ln -snf /usr/share/zoneinfo/UTC /etc/localtime && echo UTC > /etc/timezone \
    && ln -s /usr/lib/$(arch)-linux-gnu/libprofiler.so.0.4.18 /usr/lib/$(arch)-linux-gnu/libprofiler.so \
    && if [ "$GC64" = "OFF" ]; then export gc64=""; else export gc64="-gc64"; fi \
    && curl -L https://download.tarantool.org/tarantool/release/series-2/gpgkey | apt-key add - \
    && curl -L https://download.tarantool.org/tarantool/modules/gpgkey | apt-key add - \
    && echo "deb https://download.tarantool.org/tarantool/release/series-2${gc64}/ubuntu/ $(lsb_release -s -c) main" >> /etc/apt/sources.list.d/tarantool_2.list \
    && echo "deb-src https://download.tarantool.org/tarantool/release/series-2${gc64}/ubuntu/ $(lsb_release -s -c) main" >> /etc/apt/sources.list.d/tarantool_2.list \
    && echo "deb https://download.tarantool.org/tarantool/modules/ubuntu/ $(lsb_release -s -c) main" >> /etc/apt/sources.list.d/tarantool_2.list \
    && echo "deb-src https://download.tarantool.org/tarantool/modules/ubuntu/ $(lsb_release -s -c) main" >> /etc/apt/sources.list.d/tarantool_2.list \
    && apt update \
    && apt show -a tarantool \
    && apt -y install tarantool=${TARANTOOL_FULL_VERSION} \
    && apt -y install tarantool-dev=${TARANTOOL_FULL_VERSION} \
    && apt clean \
    && mkdir /.rocks \
    && ${ROCKS_INSTALLER} install lua-term \
    && ${ROCKS_INSTALLER} install avro-schema $LUAROCK_AVRO_SCHEMA_VERSION \
    && ${ROCKS_INSTALLER} install expirationd $LUAROCK_EXPERATIOND_VERSION \
    && ${ROCKS_INSTALLER} install queue $LUAROCK_QUEUE_VERSION \
    && ${ROCKS_INSTALLER} install connpool $LUAROCK_CONNPOOL_VERSION \
    && ${ROCKS_INSTALLER} install vshard $LUAROCK_VSHARD_VERSION \
    && ${ROCKS_INSTALLER} install http $LUAROCK_HTTP_VERSION \
    && ${ROCKS_INSTALLER} install pg $LUAROCK_TARANTOOL_PG_VERSION \
    && ${ROCKS_INSTALLER} install mysql $LUAROCK_TARANTOOL_MYSQL_VERSION \
    && ${ROCKS_INSTALLER} install memcached $LUAROCK_MEMCACHED_VERSION \
    && ${ROCKS_INSTALLER} install metrics $LUAROCK_METRICS_VERSION \
    && ${ROCKS_INSTALLER} install gis $LUAROCK_TARANTOOL_GIS_VERSION \
    && ${ROCKS_INSTALLER} install gperftools $LUAROCK_TARANTOOL_GPERFTOOLS_VERSION \
    && apt purge -y unzip git gcc cmake \
    && apt -y autoremove \
    && rm -rf /var/lib/apt/lists/

RUN chown tarantool /etc/tarantool

FROM tarantool-base AS cartridge-base

# install dependencies
RUN set -e; \
    ulimit -n 1024 && \
    apt-get -y update && \
    apt-get -y install build-essential cmake make gcc git unzip cartridge-cli && \
    apt-get -y clean
RUN cartridge version

# build and run
FROM cartridge-base AS cartridge-app
ARG CARTRIDGE_SRC_DIR="cartridge"
ARG TARANTOOL_WORKDIR="/app"
ARG TARANTOOL_RUNDIR="/tmp/run"
ARG TARANTOOL_DATADIR="/tmp/data"
ARG TARANTOOL_LOGDIR="/tmp/log"
ARG TARANTOOL_INSTANCES_FILE="./instances.yml"
ARG TARANTOOL_CLUSTER_COOKIE
ARG START_DELAY="5s"
ENV START_DELAY=$START_DELAY
ENV TARANTOOL_WORKDIR=$TARANTOOL_WORKDIR
ENV TARANTOOL_RUNDIR=$TARANTOOL_RUNDIR
ENV TARANTOOL_DATADIR=$TARANTOOL_DATADIR
ENV TARANTOOL_LOGDIR=$TARANTOOL_LOGDIR
ENV TARANTOOL_INSTANCES_FILE=$TARANTOOL_INSTANCES_FILE
ENV TARANTOOL_CLUSTER_COOKIE=$TARANTOOL_CLUSTER_COOKIE
ENV CMAKE_DUMMY_WEBUI="YES"
COPY $CARTRIDGE_SRC_DIR $TARANTOOL_WORKDIR
WORKDIR $TARANTOOL_WORKDIR

RUN rm -rf .rocks && cartridge build --verbose

CMD if [ -z "$TARANTOOL_CLUSTER_COOKIE" ]; then unset TARANTOOL_CLUSTER_COOKIE ; fi ; \
    sleep $START_DELAY && cartridge start --run-dir=$TARANTOOL_RUNDIR --data-dir=$TARANTOOL_DATADIR \
        --log-dir=$TARANTOOL_LOGDIR --cfg=$TARANTOOL_INSTANCES_FILE
