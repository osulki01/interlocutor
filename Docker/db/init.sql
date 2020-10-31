CREATE USER dev_user;

CREATE DATABASE articles;
GRANT ALL PRIVILEGES ON DATABASE articles TO dev_user;

CREATE TABLE test_table(user_id int);
GRANT ALL PRIVILEGES ON TABLE test_table TO dev_user;