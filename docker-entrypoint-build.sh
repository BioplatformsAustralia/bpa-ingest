#!/bin/bash

info () {
    printf "\r  [\033[00;34mINFO\033[0m] %s\n" "$1"
}

trap exit SIGHUP SIGINT SIGTERM
env | grep -iv PASS | sort

# prepare a lambda compatible Zip of build
if [ "$1" = 'releaselambdazip' ]; then
    info "[Run] Preparing a release Lambda zip"
    info "BUILD_VERSION ${BUILD_VERSION}"
    info "PROJECT_SOURCE ${PROJECT_SOURCE}"

    set -e
    rm -rf /app/*

    # clone and install the app
    set -x
    cd /app
    git clone --depth=1 --branch="${GIT_BRANCH}" "${PROJECT_SOURCE}" .
    git rev-parse HEAD > .version
    cat .version
    set +x

    # vars for creating release lambda zip
    ZIPFILE="/data/${PROJECT_NAME}-${BUILD_VERSION}.zip"

    info "ZIPFILE ${ZIPFILE}"

    # create AWS lambda deployment Zip from /env
    cd /env/lib/python3.6/site-packages/ && rm -f "${ZIPFILE}" && zip -qr9 "${ZIPFILE}" .
    cd /app/ && zip -gqr9 "${ZIPFILE}" bpaingest

    info "$(ls -lath "${ZIPFILE}")"
    exit 0
fi

info "[RUN]: Builtin command not provided [releaselambdazip]"
info "[RUN]: $*"

exec "$@"
