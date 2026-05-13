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
* So at first look, you say "then i don't need the volume if python is one who saving i just re-write teh command to save in my local file ./artifacts for example"
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
* this line make MLFLOW SERVER as middleman, your script pload the model to the server over HTTP (8080) and server will place the model inside the container it self and the container is already mounted to my volume so here the artifacs is presistant even if we stop and run the container again and MLFLOW know where the artifact exists
* in production better to use AWS S3, GCP Storage, Azure Blob