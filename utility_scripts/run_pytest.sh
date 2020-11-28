# Run unit tests first
docker exec recommender_stg /home/docker_user/.local/bin/pytest --verbose -m 'not integration' interlocutor/

# Run integration tests next
docker exec recommender_stg /home/docker_user/.local/bin/pytest --verbose -m 'integration' interlocutor/