# Run tests to track coverage
docker exec recommender_stg /home/docker_user/.local/bin/coverage run -m pytest interlocutor/

# Produce report
docker exec recommender_stg /home/docker_user/.local/bin/coverage report -m

# Produce visual report
docker exec recommender_stg /home/docker_user/.local/bin/coverage html