# Tracker
TBA

## Instructions

TBA

### Deploy
* Get a certificate:
    * Deploy on AWS with config parameters in conjunction with KMS on ECS on Amazon AWS https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b (see dockercmd.sh)
    * Use the above test key **must be server.crt and server.key**
    * Copy a server.crt and server.key in from sn SSL certificate provider.
* Update **config.json** to **UseLocalTLS=true** if required.
* Deploy on Docker using the following:
```
# Build from src:
sudo docker build -t tracker .
# Deploy only:
# sudo docker build -f Dockerfile.deploy -t tracker .
sudo docker run -p 8443:443 tracker
# Connect to it:
#  sudo docker ps
#  sudo docker exec -it [container_id] bash
# Remove all your images (warning):
#  sudo docker system prune -a
```
* Then upload/use (try AWS ECS).

## Credits
* [DragonGate](https://github.com/dioptre/DragonGate)
* [SF Product Labs](https://sfproductlabs.com)

