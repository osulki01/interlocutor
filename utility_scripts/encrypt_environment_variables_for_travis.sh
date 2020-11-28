tar cvf Docker/environment_variables/secrets.tar Docker/environment_variables/.env.dev Docker/environment_variables/.env.stg Docker/environment_variables/.env.prd
travis encrypt-file --pro --force Docker/environment_variables/secrets.tar
mv secrets.tar.enc Docker/environment_variables/secrets.tar.enc
