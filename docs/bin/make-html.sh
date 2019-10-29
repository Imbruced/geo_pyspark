#! /bin/bash

# ------------------------------------------------------------------------------
# Compile Sphinx documentation to HTML.
#
# Examples:
#   ./make-html
# ------------------------------------------------------------------------------

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(realpath $SCRIPT_DIR/..)"

source "$SCRIPT_DIR/commons.sh"

# The entry point of this script.
function main() {
    local options_index=1

    local build_dir="$PROJECT_DIR/build"
    local source_dir="$PROJECT_DIR/source"
    local sphinx_docker_image="ddidier/sphinx-doc:$SPHINX_DOCKER_IMAGE_VERSION"

    printf "\n"
    printf "Deleting build directory '%s'\n" "$build_dir"
    rm -rf "$PROJECT_DIR/build"

    printf "Running Sphinx image '%s'\n" "$sphinx_docker_image"
    printf "    building directory '%s'\n" "$PROJECT_DIR"
    printf "    with user ID %d\n" "$UID"
    printf "\n"

    docker run --rm \
        -e USER_ID=$UID \
        -v $PROJECT_DIR:/doc \
        $sphinx_docker_image make html
}

# Print the help message.
function help() {
    printf "Compile Sphinx documentation to HTML.\n"
    printf "\n"
    printf "Usage: ./make-html [OPTIONS]\n"
    printf "  -h, --help    Print this message and exit\n"
    printf "\n"
    printf "Examples:\n"
    printf "  ./make-html\n"
}

main "$@"
