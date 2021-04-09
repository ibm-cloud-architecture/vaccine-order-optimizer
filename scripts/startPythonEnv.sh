source ./scripts/setenv.sh

docker run -ti -e KAFKA_BROKERS=$KAFKA_BOOTSTRAP_SERVERS -e SCHEMA_REGISTRY_URL=$SCHEMA_REGISTRY_URL -e REEFER_TOPIC=$REEFER_TOPIC -e INVENTORY_TOPIC=$INVENTORY_TOPIC -e TRANSPORTATION_TOPIC=$TRANSPORTATION_TOPIC -e KAFKA_USER=$KAFKA_USER -e KAFKA_PASSWORD=$KAFKA_PASSWORD -e KAFKA_CERT=$KAFKA_CERT -e APP_VERSION=$APP_VERSION -p 5000:5000  -v $(pwd):/app ibmcase/vaccine-order-optimizer bash
