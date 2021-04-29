# interlocutor
Exploring recommendation systems which suggest content you would not have naturally considered, and may broaden the 
user's experience/mindset.

This project is work-in-progress, but intends to use news articles as an opportunity to recommend news about topics
a reader is interested in, but from a publication that they would not usually read.


## Development environment

You will need [Docker](https://docs.docker.com/get-docker/) available on your machine, as well as 
[Docker Compose](https://docs.docker.com/compose/install/) (which will generally be included in the Docker installation).

You can now establish the required Docker images/containers for each part of the service:

```shell script
# Build docker images (only if running for the first time)
docker-compose build

# Spin up all docker containers
docker-compose up -d
```

Here is an overview of what each docker service is responsible for:

* dev: Used for developing, prototyping, and running linting to ensure code quality.
* stg: Staging environment to run tests on. Exactly the same image as dev but with libraries focused on testing.
* prd: For running live service. Exactly the same image as dev but only include libraries required for execution.
* db_stg: Postgres staging database. No data is preserved from this container so it creates a new database with mock 
data specifically used for testing each time a container is created.
* db_prd: Production database. Exactly the same tables and schema as db_stg but no mock data is uploaded, and real data 
is preserved within [data/postgres/prd](data/postgres/prd).
* neo4j_stg: Experimental work exploring a graph database for storing article and reader relationships.


## CI Pipeline

All code pushed to this repo is automatically tested via [Travis CI](https://travis-ci.com/) as per the configuration in 
[.travis.yml](.travis.yml).

Note that Travis needs to read environment variables which contains sensitive information like database passwords or 
The Guardian's API key. As such, the environment variables are encrypted as per the advice 
[here](https://docs.travis-ci.com/user/environment-variables/#encrypting-environment-variables).

To start with, you need three an environment file for each type of environment (.env.dev, .env.stg, .env.prd) stored 
in [Docker/environment_variables](Docker/environment_variables). These should not be pushed to GitHub.

The utility script 
[utility_scripts/encrypt_environment_variables_for_travis.sh](utility_scripts/encrypt_environment_variables_for_travis.sh) 
handles the encryption process by placing the three files in a single local zip file before encrypting it.
As such, the file [Docker/environment_variables/secrets.tar.enc](Docker/environment_variables/secrets.tar.enc) can be 
safely pushed to GitHub for Travis to access, but never the local file secrets.tar as it contains the 
unencrypted secrets.


## Retrieving articles

Data is scraped from different publications via [interlocutor/get_data](interlocutor/get_data). Article content is 
extracted from The Guardian using their API (with credentials stored in 
[Docker/environment_variables](Docker/environment_variables)), whereas all other publications have their articles 
scraped directly from the website.

The utility script [utility_scripts/get_data.sh](utility_scripts/get_data.sh) can be used to download articles and store their content.


## Where the data is stored

All persisted data should be stored within the postgres database created in the service db_prd (or db_stg for testing).

The plan is to produce a more user-friendly entity-relationship diagram but until then, all of the tables, columns, and 
their meanings can be found in [Docker/db/01_init.sql](Docker/db/01_init.sql).


## Preprocessing articles

Articles are preprocessed so that their text content is normalised via 
[interlocutor/nlp/preprocessing.py](interlocutor/nlp/preprocessing.py)) and then represented via a TF-IDF encoding via 
[interlocutor/nlp/encoding.py](interlocutor/nlp/encoding.py)).

Using this encoding, articles can be compared to one another to see if they share similar content.
