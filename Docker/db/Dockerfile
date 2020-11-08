FROM postgres:13-alpine

# Providing the postgres data directory is empty i.e. the service is being created for the first time
# then all scripts in this directory will be executed. In this case, make sure a database and user are created.
COPY db/init.sql /docker-entrypoint-initdb.d/


# Take the postgres_user from the environment file to dynamically populate the initialisation script
# and ensure that any tables or databases created can grant permissions to that user

# Install packages which include the envsubst program, which can be used to substitute the values in a script with
# environment variables.
RUN apk add gettext libintl

# Bring in the appropriate environment file
ARG DEPLOYMENT_ENVIRONMENT
COPY environment_variables/.env.${DEPLOYMENT_ENVIRONMENT} /.env

# Substitute the postgres_user in initialisation script
RUN source .env && \
export POSTGRES_USER=$POSTGRES_USER && \
envsubst < /docker-entrypoint-initdb.d/init.sql > /docker-entrypoint-initdb.d/init_with_username.sql
RUN rm /docker-entrypoint-initdb.d/init.sql
