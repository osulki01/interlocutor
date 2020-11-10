FROM postgres:13-alpine

# Install packages which include the envsubst program, which can be used to substitute the values in a script with
# environment variables.
RUN apk add gettext libintl

# Bring in the appropriate environment file
ARG DEPLOYMENT_ENVIRONMENT
COPY environment_variables/.env.${DEPLOYMENT_ENVIRONMENT} /.env

# Providing the postgres data directory is empty i.e. the service is being created for the first time
# then all scripts in the /docker-entrypoint-initdb.d/ directory will be executed.
COPY db/01_init.sql /docker-entrypoint-initdb.d/
COPY db/02_init_${DEPLOYMENT_ENVIRONMENT}.sql /docker-entrypoint-initdb.d/

# Substitute the postgres_user in initialisation script
RUN source .env && \
export POSTGRES_USER=$POSTGRES_USER && \
envsubst < /docker-entrypoint-initdb.d/01_init.sql > /docker-entrypoint-initdb.d/01_init_with_username.sql
RUN rm /docker-entrypoint-initdb.d/01_init.sql
