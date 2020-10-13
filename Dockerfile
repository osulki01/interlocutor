FROM postgres:latest AS postgres_base

FROM python:3.7

# Ensure setup from base postgres image is available
COPY --from=postgres_base . .

# Add user with sudo privileges
ARG username=docker_user
RUN adduser --disabled-password --gecos '' $username
RUN adduser $username sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER $username

WORKDIR /usr/src/app
# Ensure current host directory containing python requirements is available and create virtual environment
COPY . .
RUN pip install -r python_requirements/requirements.txt

# Keep container running in detached mode, so execute a meaningless command in the foreground
CMD tail -f /dev/null
