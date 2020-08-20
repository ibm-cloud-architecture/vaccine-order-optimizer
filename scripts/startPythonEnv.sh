
if [[ $# -eq 0 ]];then
  kcenv="LOCAL"
else
  kcenv=$1
fi
source ./scripts/setenv.sh $kcenv

if [[ $kcenv == "LOCAL" ]]
then
  docker run  -v $(pwd):/home -e KAFKA_BROKERS=$KAFKA_BROKERS \
     --network kafkanet \
      -ti ibmcase/python37 bash
else
  docker run  -v $(pwd):/home -e KAFKA_BROKERS=$KAFKA_BROKERS \
     -e KAFKA_APIKEY=$KAFKA_APIKEY -e KAFKA_CERT=$KAFKA_CERT\
      -ti ibmcase/python37  bash
fi
