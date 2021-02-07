FROM neo4j:latest

# Bring in the appropriate environment file
ARG DEPLOYMENT_ENVIRONMENT

# Make CSVs available to copy into database
COPY neo4j/${DEPLOYMENT_ENVIRONMENT}_data/*.csv /var/lib/neo4j/import
