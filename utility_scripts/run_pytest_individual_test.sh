# Run pytest for single test specified by keyword expression
# https://docs.pytest.org/en/stable/usage.html#specifying-tests-selecting-tests
docker exec recommender_stg /home/docker_user/.local/bin/pytest interlocutor/ --verbose -k "$1"
