FROM python:3.7

# Install pipenv is available to run python virtual environments
RUN ["/usr/local/bin/pip", "install", "pipenv"]

# Add user with sudo privileges
ARG username=docker_user
RUN adduser --disabled-password --gecos '' $username
RUN adduser $username sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER $username

# Ensure current host directory containing Pipfile.lock is available and create virtual environment
WORKDIR /usr/src/app
COPY . .
RUN pipenv sync --dev

# Keep container running in detached mode, so execute a meaningless command in the foreground
CMD tail -f /dev/null
