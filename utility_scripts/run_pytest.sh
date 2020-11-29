# Run unit tests first
echo "******************************************"
echo "Running unit tests"
echo "******************************************"
docker exec recommender_stg /home/docker_user/.local/bin/pytest --verbose -m 'not integration' interlocutor/

# Run integration tests next
echo "******************************************"
echo "Running integration tests"
echo "******************************************"
docker exec recommender_stg /home/docker_user/.local/bin/pytest --verbose -m 'integration' interlocutor/