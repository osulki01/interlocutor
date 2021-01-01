FROM python:3.7

# Add user with sudo privileges
ARG username=docker_user
RUN adduser --disabled-password --gecos '' $username
RUN adduser $username sudo
RUN ["echo", "%sudo ALL=(ALL) NOPASSWD:ALL", ">>", "/etc/sudoers"]
USER $username

# Ensure current host directory containing python requirements is available and create virtual environment
ARG working_directory=/usr/src/app
WORKDIR $working_directory
COPY . .
ARG DEPLOYMENT_ENVIRONMENT
RUN pip install --user --requirement python_requirements/${DEPLOYMENT_ENVIRONMENT}_requirements.txt
RUN python -m spacy download en_core_web_sm

# Ensure the local namespace python packages can be imported and used from anywhere
ENV PYTHONPATH="${PYTHONPATH}:${working_directory}"

# Keep container running in detached mode, so execute a meaningless command in the foreground
CMD ["tail", "-f", "/dev/null"]
