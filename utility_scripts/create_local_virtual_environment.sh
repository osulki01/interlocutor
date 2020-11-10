# Delete and replace virtual environment if it already exists
[ -d ".docker_venv" ] && rm -r .docker_venv
python3 -m venv .docker_venv
source .docker_venv/bin/activate

pip install -r ./Docker/recommender/python_requirements/dev_requirements.txt

# Exit virtual environment
deactivate