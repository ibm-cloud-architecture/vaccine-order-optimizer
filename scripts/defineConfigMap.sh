ns=$(oc project --short)
if [[ $# -eq 0 ]];then
  echo "Usage: namespace needed, use the default one"
  echo $ns
else
  ns=$1
fi

cmfound=$(kubectl get  cm --field-selector=metadata.name=kafka-brokers)

if [[ $cmfound == *"No resources found"* ]];then
    kubectl create configmap kafka-brokers --from-literal=brokers=$KAFKA_BROKERS -n $ns
fi
kubectl describe configmap kafka-brokers -n $ns

secfound=$(kubectl get  secrets --field-selector=metadata.name=eventstreams-apikey)
if [[ $cmfsecfoundound == *"No resources found"* ]];then
    kubectl create secret generic eventstreams-apikey --from-literal=binding=$KAFKA_APIKEY -n $ns
    sleep 5
fi
kubectl describe secret eventstreams-apikey -n $ns