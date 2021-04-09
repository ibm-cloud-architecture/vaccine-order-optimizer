docker exec -ti  kafka  bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create  --replication-factor 1 --partitions 1 --topic vaccine.reefers"

docker exec -ti  kafka  bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create  --replication-factor 1 --partitions 1 --topic vaccine.inventory"

docker exec -ti  kafka  bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create  --replication-factor 1 --partitions 1 --topic vaccine.transportation"

docker exec -ti  kafka  bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create  --replication-factor 1 --partitions 1 --topic vaccine.orderevents"