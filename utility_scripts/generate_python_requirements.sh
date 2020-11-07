#.venv/bin/pip-compile --output-file ./Docker/dev/python_requirements/requirements.txt ./Docker/dev/python_requirements/requirements.in

# Delete and replace virtual environment if it already exists
[ -d ".venv" ] && rm -r .venv
python3 -m venv .venv
source .venv/bin/activate

# Enter virtual environment to use pip-compile-multi and
# compile multiple requirements files to lock dependency versions
source .venv/bin/activate
pip install pip-compile-multi
pip-compile-multi --directory Docker/recommender/python_requirements

# Exit virtual environment
deactivate