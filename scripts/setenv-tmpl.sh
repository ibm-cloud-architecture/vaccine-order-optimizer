#####################
## Main properties ##
#####################
# Set these regardless of where your Event Streams on prem or on IBM Cloud or a local Kafka instance
export KAFKA_BROKERS=""
export REEFER_TOPIC="vaccine-reefer"
export INVENTORY_TOPIC="vaccine-inventory"
export TRANSPORTATION_TOPIC="vaccine-transportation"
export SCHEMA_REGISTRY_URL=
######################
## OCP and IBMCLOUD ##
######################
# Set these if you are using Event Streams on prem or on IBM Cloud
export KAFKA_USER=""
export KAFKA_PASSWORD=""
export APP_VERSION=v0.0.3
#########
## OCP ##
#########
# Set the SSL certificate location if you are working against an Event Streams instance on OCP
# Below where appsody will place the certificates you include in the certs folder of this project
# If you are building the docker image yourself and then running it standalone or through docker compose, you
# will most likely need to update the cert path
export KAFKA_CERT="/app/certs/es-cert.pem"

