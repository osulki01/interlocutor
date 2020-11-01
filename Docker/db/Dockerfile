FROM postgres:13-alpine

# Providing the postgres data directory is empty i.e. the service is being created for the first time
# then all scripts in this directory will be executed. In this case, make sure a database and user are created.
COPY init.sql /docker-entrypoint-initdb.d/
