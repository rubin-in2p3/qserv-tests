#!/bin/bash

# Create docker image containing scaletests tools and scripts

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

docker build --build-arg BASE_IMAGE="$BASE_IMAGE" --target scaletests-deps -t "$DEPS_IMAGE" "$DIR"
docker image build --build-arg BASE_IMAGE="$BASE_IMAGE" --tag "$IMAGE" "$DIR"
docker tag "$IMAGE" "$IMAGE_NAME:latest" 
docker push "$IMAGE"
docker push "$IMAGE_NAME:latest"
docker push "$DEPS_IMAGE"
