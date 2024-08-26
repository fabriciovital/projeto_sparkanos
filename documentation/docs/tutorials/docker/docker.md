# Docker

## Login to Docker HUB
```
sudo docker login
```

## Pull the image from Docker HUB
```
sudo docker pull wlcamargo/jupyter
```

## Run the container in interactive mode
```
sudo docker run -it --name jupyter wlcamargo/jupyter /bin/bash
```

## Access the container
```
sudo docker exec -it jupyter /bin/bash
```

## Run a specific container
```
sudo docker compose up -d jupyter
```

## Stop a specific container
```
sudo docker stop -d jupyter
```

## Copy file from host to container
```
docker cp file.jar jupyter:/opt/spark/jars/
```

## Copy file from container to host
```
docker cp jupyter:/opt/spark/jars/
```

## Clone Docker Image

### Tag the image
```
docker tag wlcamargo/spark-master your-repository/image
```

## Docker login
```
docker login
```

Add your credentials

### Push the image
```
docker push your-repository/image
```

### Disable Firewall
```
sudo ufw disable
```

## Pull image
Sample Nginx
```
docker pull nginx
```

## Tag image
```
docker tag nginx wlcamargo/nginx-sparkanos:v1
```

# Docker Swarm Mode
## Init 
```
docker swarm init --advertise-addr <ip do host>
```

## Create Stack
```
docker stack deploy -c docker-compose-swarm.yml big_data
```

