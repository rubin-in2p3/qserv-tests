GIT_HASH="$(git describe --dirty --always)"
TAG=${OP_VERSION:-${GIT_HASH}}

BASE_IMAGE="python:3.9-buster"
# Image version created by build procedure
IMAGE_NAME="qserv/scaletests"
IMAGE="$IMAGE_NAME:$TAG"
DEPS_IMAGE="$IMAGE_NAME-deps:latest"
