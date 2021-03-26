#!/bin/bash
scriptDir=$(dirname $0)

curl -X POST -H "Content-type: application/json; artifactType=AVRO" \
   -H "X-Registry-ArtifactId: vaccine.reefers-value" \
   --data ${scriptDir}/data/avro/schemas/reefer.avsc http://localhost:8080/api/artifacts

curl -X POST -H "Content-type: application/json; artifactType=AVRO" \
   -H "X-Registry-ArtifactId: vaccine.inventory-value" \
   --data ${scriptDir}/data/avro/schemas/inventory.avsc http://localhost:8080/api/artifacts

curl -X POST -H "Content-type: application/json; artifactType=AVRO" \
   -H "X-Registry-ArtifactId: vaccine.transportation-value" \
   --data ${scriptDir}/data/avro/schemas/transportation.avsc http://localhost:8080/api/artifacts

curl -X POST -H "Content-type: application/json; artifactType=AVRO" \
   -H "X-Registry-ArtifactId: cloudEvent" \
   --data ${scriptDir}/data/avro/schemas/cloudEvent.avsc http://localhost:8080/api/artifacts