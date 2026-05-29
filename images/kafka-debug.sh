cat /etc/consumer-credentials/user.key /etc/consumer-credentials/user.crt > /tmp/user-keystore.pem
cat <<EOF > /tmp/client.properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/cluster-ca/ca.crt
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/user-keystore.pem
EOF
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.default.svc:9093 \
  --topic crypto-prices \
  --command-config /tmp/client.properties \
  --group "my-group" \
  --from-beginning