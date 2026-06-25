# Python + Mlflow Docker
```bash
mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    environment:
      MLFLOW_BACKEND_STORE_URI: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}
      MLFLOW_DEFAULT_ARTIFACT_ROOT: /mlflow/artifacts  # this tell the mlflow server where to store the artifacts
    ports:
      - "8080:5000"
    volumes:
      - mlflow_artifact_volume:/mlflow/artifacts
command: >
    mlflow server 
      --backend-store-uri postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}
      --default-artifact-root /mlflow/artifacts 
      --host 0.0.0.0
      --port 5000

```

because mlflow is inside container and my python script is in my local pc, i have faced this problem

## Problem + solution
### problem

* Mlflow server is not responsible for saving the artifact this is python script problem is the one responsible for saving the artifacts
* As you can see above the file for storing the artifacte inside a container is set in the enviroment vairable
* But as i said the one is saving the artifact is the python, and python doesn't have access inside the mlflow container
* So at first look, you say "then i don't need the volume if python is one who saving i just re-write the command to save in my local file ./artifacts for example"
* Still gona face problem because if you did that it will work at first, but the moment you stop the container and run it again you will have the problem
* The problem is Mlflow don't have access to the model because it doesn't know where it exists

### Solution
* Make the one who is saving the artifact is MLFLOW
* add this line of command to the original command " --serve-artifacts "
```bash
    -backend-store-uri postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}
    --default-artifact-root /mlflow/artifacts
    --serve-artifacts
    --host 0.0.0.0
    --port 5000
```
* this line make MLFLOW SERVER as middleman, your script load the model to the server over HTTP (8080) and server will place the model inside the container it self and the container is already mounted to my volume so here the artifacs is presistant even if we stop and run the container again and MLFLOW know where the artifact exists
* in production better to use AWS S3, GCP Storage, Azure Blob

## kafka Connect Configuration
### plugin.name inside config
* when you insert or update or delete data in PostgresSQL, Postgres writes that change down in a secret internal diary called the WAL (Write-Ahead Log)
* Kafka Cannot read Postgres internal diary direclty, it needs a translator "plugin"
* if you didn't use this configuration then the default behaviour happen which is the batch mode
* when 10 rows updated the plugin translate these 10 rows into single batch and hands it to Debezium
* The problem when you update/create 10 million rows, here you will face " Out of Memory "
* **Soulution**:
* use Streaming Mode instead of Batch Mode
* either the built in inside Postgres " **pgoutput** " the one i already used
* or downlaod a third party one like " **wal2json_streaming** "
## slot.name & slot.drop.on.stop
* Postgres allows you to have multiple bookmarks at the same time
* These two give you the feature of bookmarking your internal diary WAL, it tells kafka connect exactly which change Debezium has already read and which ones it hasn't
* **slot.name** -> just make you name your bookmark if you don't use it by default it called debezium, you usually use it when you have multiple different Debezium connectors against the exact same Postgres database
* **slot.drop.on.stop** -> either true or false, by default is false which means if server crash it doesn't delete the WAL because it assume that it will eventually come back online so it resume from where it stop
* But if it set to ture, if crash immediatly delet the bookmarks, so this is used while developing for testing, becuase you don't want every time adding feature or changing feature or continuo developing to work from where it stop becasue during developing you stop alot
## publication.name
* If you choose the **pgoutput** then you have to use this configuration
* If you leave it empty, Debizum will connect to Postgres and say " hey, create a brand new channel called **dez_publication** and broadcast every single table in the entire database on it", for most people this is excatly what they want
* The Custom behavior: let's say your database has 50 tables, but you only want kafka to know about the **user** and **order** tables. You can log into your Postgres database yourself and type a SQL command to create a custom channel **CREATE PUBLICATION my_custom_channel FOR TABLE user, order;**, then use this channle as the publication.name, now debizum will ignore the other 48 tables
## include/exclude [schema - table - columns]:
* You ether choose include or exclude not use both
* **A - Schema:**
* put entire schema or exclude it if you have more than one schema
* normally you have one schema which is **public** and it is automatically created by postgress, and the use of schema is when you need seperation or control over the database for example if you want espicific department to have specific tables in dtatabase you can create "public - accounting -engineers" meaning there is tables are for accountants and other for engineer and so on
* **B - Table:**
* include or exclud tables , seperated by comma
``` JSON
"table.include.list": "public.your_new_table"
```
* **C - Columns**
### the name of the topic:
* topic.prefexi + schema_name + table_name
* In this project is called => **pg.public.air_pollution**

## Prefect
### A) Dependencies in docker-compose Problem:
* first time i just used the image and then use the (RUN) command to run the script after i just used the volum to copy it inside the contain
* the problem with this approach is the run command will execute the file and the first problem you gonna face is **pandas/numpy/..** not found
* because the container i created only contain the prefect
### a) Dependencies in docker compose solution:
* create a dockerfile that contain the prefect and the other dependncies that needed for the file to run smoothly
* use the dockerfile in the docker compose

### B) Volumes in docker compose:
* there is three service in my docker compose the first one just to run the server
* second one do two things create the pool and run the script **Register the script** to be percies
* because the code is saying **main.deploy() not main()** this means the code is regitered to be run later not run immedialty if it said main() this means run immediatly and this isn't the case
* Thats why the third service the **prefect-worker** is the one who will run the main() when the scheduled time come and this means this container gonna save the html file thats why the volumns here is more important than to be in the second continer in the docker-compose file

### C) Immutable Code:
#### dockerfile:
* i had two options either copy my code insided the dockerfile and create immutable image, 
* problem with this approach is if i have to change something in the code i had to stop the container and then updated and rebuild it and then run again
* the other option is to just create the image using the dockefile and make the image already holding the dependecies it needs like (pandas - mlflow - ...) so that only need to fetch the code and the container have everything ready for it to run
* this is better because you don't need to stop the container the only thing need is to update the code and re-upload it to github and thats is it

## Creating docker image
* if i copy src ./src in the docker file in this case i don't need to deploy the flow from github
* because the registration is done when i did **docker compose up -d** when the second service run the script **python -m src.pipeline.dirft_check.py**
* the above line of command will run the script one time to register the info in the pool
* now when i run from github it will work but if i change the **corn** for example it won't change because the registration in first place was from inside the container the code lives there when i did **copy src ./src**

## Mlflow
* first time the mlflow was running in local host during development
* after dockerizing we need to connect using the docker network, hence **--host 0.0.0.0** which expose the service to other container
* using the service name as host **http://main_flow:5000**