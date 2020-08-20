if [[ $# -eq 0 ]];then
  kcenv="LOCAL"
else
  kcenv=$1
fi

export REEFERS_TOPIC=reefers

case "$kcenv" in
   OCP)
    export KAFKA_BROKERS=:443
    export KAFKA_USER=ext-serv
    # get the password: oc get secret ext-serv -o jsonpath='{.data.password}' | base64 --decode
    export KAFKA_PWD=
    export KAFKA_CERT=/home/certs/es-cert.pem
    ;;
   LOCAL)
    export KAFKA_BROKERS=kafka:9092
    ;;
   CLOUD)
  export KAFKA_PWD=
  export KAFKA_USER=token
  export KAFKA_BROKERS=
   ;;
esac