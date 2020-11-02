FROM python:3.7

# Add user with sudo privileges
ARG username=docker_user
RUN adduser --disabled-password --gecos '' $username
RUN adduser $username sudo
RUN ["echo", "%sudo ALL=(ALL) NOPASSWD:ALL", ">>", "/etc/sudoers"]
USER $username

# Ensure current host directory containing python requirements is available and create virtual environment
WORKDIR /usr/src/app
COPY . .
RUN ["pip", "install", "-r", "python_requirements/requirements.txt"]

# Keep container running in detached mode, so execute a meaningless command in the foreground
CMD ["tail", "-f", "/dev/null"]
