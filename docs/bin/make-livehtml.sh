#! /bin/bash

# ------------------------------------------------------------------------------
# Run Sphinx in live HTML mode. Sphinx will watch the project directory and
# rebuild the documentation when a change is detected. The documentation will
# be available at http://localhost:<port>. The default port is 8000.
#
# Examples:
#   ./make-livehtml
#   ./make-livehtml --name my-documentation --interface 0.0.0.0 --port 9000
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
    local sphinx_docker_expose_interface=$SPHINX_DOCKER_EXPOSE_INTERFACE
    local sphinx_docker_expose_port=$SPHINX_DOCKER_EXPOSE_PORT

    while [[ $options_index -le $# ]]; do

        case "${!options_index}" in
            -i|--interface)
                options_index=$((options_index + 1))
                sphinx_docker_expose_interface="${!options_index}"
                ;;
            -n|--name)
                options_index=$((options_index + 1))
                sphinx_docker_name="${!options_index}"
                ;;
            -p|--port)
                options_index=$((options_index + 1))
                sphinx_docker_expose_port="${!options_index}"
                ;;
            -h|--help)
                help
                exit 0
                ;;
            *)
                fatal "Invalid argument '${!options_index}'"
                ;;
        esac

        options_index=$((options_index + 1))
    done

    if [[ -z "$sphinx_docker_expose_interface" ]]; then
        fatal "The exposed interface cannot be empty. Please use '-i' or '--interface'."
    fi

    if [[ -z "$sphinx_docker_name" ]]; then
        fatal "The name cannot be empty. Please use '-n' or '--name'."
    fi

    if [[ -z "$sphinx_docker_expose_port" ]]; then
        fatal "The exposed port cannot be empty. Please use '-p' or '--port'."
    fi

    printf "\n"
    printf "Deleting build directory '%s'\n" "$build_dir"
    rm -rf "$PROJECT_DIR/build"

    printf "Running Sphinx image '%s'\n" "$sphinx_docker_image"
    printf "    building directory '%s'\n" "$PROJECT_DIR"
    printf "    running in container '%s'\n" "$sphinx_docker_name"
    printf "    listening on interface %s\n" "$sphinx_docker_expose_interface"
    printf "    listening on port %d\n" "$sphinx_docker_expose_port"
    printf "    with user ID %d\n" "$UID"
    printf "\n"

    # echo -e "Docker command: \n \
    # docker run --rm -it \n \
    #     -e USER_ID=$UID \n \
    #     --name $sphinx_docker_name \n \
    #     -p $sphinx_docker_expose_interface:$sphinx_docker_expose_port:$sphinx_docker_expose_port \n \
    #     -v $PROJECT_DIR:/doc \n \
    #     $sphinx_docker_image \n \
    #         make SPHINXPORT=$sphinx_docker_expose_port livehtml"

    docker run --rm -it \
        -e USER_ID=$UID \
        --name $sphinx_docker_name \
        -p $sphinx_docker_expose_interface:$sphinx_docker_expose_port:$sphinx_docker_expose_port \
        -v $PROJECT_DIR:/doc \
        $sphinx_docker_image \
            make SPHINXPORT=$sphinx_docker_expose_port livehtml
}

# Print the help message.
function help() {
    printf "Run Sphinx in live HTML mode. Sphinx will watch the project directory and\n"
    printf "rebuild the documentation when a change is detected. The documentation will\n"
    printf "be available at http://localhost:<port>. The default port is 8000.\n"
    printf "\n"
    printf "Usage: ./make-livehtml [OPTIONS]\n"
    printf "  -i, --interface INTERFACE\n"
    printf "                    The interface on the host machine Sphinx is bound to\n"
    printf "                    The default interface is $SPHINX_DOCKER_EXPOSE_INTERFACE\n"
    printf "  -n, --name NAME\n"
    printf "                    The name of the container\n"
    printf "                    The default name is $SPHINX_DOCKER_NAME\n"
    printf "  -p, --port PORT\n"
    printf "                    The port on the host machine Sphinx is bound to\n"
    printf "                    The default port is $SPHINX_DOCKER_EXPOSE_PORT\n"
    printf "  -h, --help\n"
    printf "                    Print this message and exit\n"
    printf "\n"
    printf "Examples:\n"
    printf "  ./make-livehtml\n"
    printf "  ./make-livehtml --name my-documentation --interface 0.0.0.0 --port 9000\n"
}

main "$@"
