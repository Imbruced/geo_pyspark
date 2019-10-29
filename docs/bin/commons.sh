#! /bin/bash

# ------------------------------------------------------------------------------
# This file is sourced by the scripts in the same directory.
# ------------------------------------------------------------------------------

# The version of the Sphinx image
SPHINX_DOCKER_IMAGE_VERSION="latest"

# The name of the container Sphinx is running in
SPHINX_DOCKER_NAME="ndd-docker-sphinx"
# The network interface the host machine Sphinx is bound to
SPHINX_DOCKER_EXPOSE_INTERFACE=127.0.0.1
# The port on the host machine Sphinx is bound to
SPHINX_DOCKER_EXPOSE_PORT=8000

# Print a specific message then the help message then exit with 1.
# @param $1 message the specific message.
function fatal() {
    local message="$1"
    printf "FATAL: %s\n\n" "$message" >&2
    help
    exit 1
}
