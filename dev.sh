#!/usr/bin/env bash

# Usage: source dev.sh

REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${REPO_DIR}" || exit

deactivate || true

pip3 install virtualenv
python3 -m venv ~/.lightscript-venv
. ~/.lightscript-venv/bin/activate

pip3 install --upgrade pip

# install pre-commit
pip3 install pre-commit
pre-commit install

pip3 install -r requirements.txt

export PYTHONPATH=$(pwd):$PYTHONPATH
