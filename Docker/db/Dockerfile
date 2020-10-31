FROM postgres:13-alpine

COPY init.sql /docker-entrypoint-initdb.d/
#ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
#ENTRYPOINT ["docker-entrypoint.sh"]

#ENV POSTGRES_PASSWORD=xxxx
#ENV POSTGRES_USER=postgres_user
#ENV POSTGRES_DB=first_db
#RUN bash usr/local/bin/docker-entrypoint.sh 'postgres'

#RUN POSTGRES_PASSWORD=xxxx /usr/local/bin/docker-entrypoint.sh

# Keep container running in detached mode, so execute a meaningless command in the foreground
#CMD ["tail", "-f", "/dev/null"]