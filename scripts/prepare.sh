#!/bin/bash

set -e

# Note: when running the prepare script, all relevant modules should already be loaded

#=====================================
# Create and/or load virtual environment
#=====================================
resilipipe_base_dir="$(dirname $(dirname $( realpath $0)))"
export RESILIPIPE_DIR="${resilipipe_base_dir}/resilipipe/resilipipe"

if ! [[ -f "$resilipipe_base_dir/.venv/bin/activate" ]] ; then
    echo "Creating virtual environment under $resilipipe_base_dir/.venv"
    python -m venv "$resilipipe_base_dir/.venv"
fi

source "$resilipipe_base_dir/.venv/bin/activate"

#=====================================
# Install necessary packages and prepare modules
#=====================================
pip3 install "$resilipipe_base_dir/resilipipe"
python -m resilipipe.parse.prepare_modules

echo
echo "Completed preparation!"
echo

deactivate
