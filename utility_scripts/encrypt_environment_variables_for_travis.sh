travis encrypt-file --pro --force Docker/environment_variables/.env.dev
mv .env.dev.enc Docker/environment_variables/.env.dev.enc

travis encrypt-file --pro --force Docker/environment_variables/.env.stg
mv .env.stg.enc Docker/environment_variables/.env.stg.enc

travis encrypt-file --pro --force Docker/environment_variables/.env.prd
mv .env.prd.enc Docker/environment_variables/.env.prd.enc