# KafkaSample

Very simple Java client to verify Kafka connectivity in Hadoop/HDP environment. Three functions and corresponding bash scripts:
* list.sh List topics 
* produce.sh Produce stream to Kafka topic.
* consume.sh Consume Kafka stream.

# Configuration
>cd sh<br>
>  cp templates/* .<br>

Adjust config files according to your environment.

https://github.com/stanislawbartkowski/KafkaSample/blob/master/sh/templates/kafka.properties<br>
https://github.com/stanislawbartkowski/KafkaSample/blob/master/sh/templates/env.rc<br>

# Usage
## List topics

> cd sh<br>
> ./list.sh<br>

The output should be similar to output produced by *kafka-topics.sh --list*

## Produce stream

>cd sh<br>
>./produce.sh

Verify the stream by *consume* (look below) or by standard *kafka-console-consumer.sh*
> /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh  --bootstrap-server mdp1.sb.com:6667 --topic test_topic

## Consume stream
Start producing messages by *produce* (look above) or by standard *kafka-console-producer.sh*
> /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh  --broker-list mdp1.sb.com:6667 --topic test_topic

# Kerberos
**sh/env.rc** Uncomment and modify the *KERBEROS* variable.

Modify *kafka_client_jaas.conf* accordingly. In HDP 3.1 it could be: /etc/kafka/3.1.0.0-78/0/kafka_client_jaas.conf<br>
Before running the test, the proper Kerberos ticket should be obtained.<br>

For Kerberos troubleshooting, modify the KERBEROS variable.
* KERBEROS="-Djava.security.auth.login.config=/etc/kafka/3.1.0.0-78/0/kafka_client_jaas.conf -Dsun.security.krb5.debug=true"
<br>
```
KERBEROS=-Djava.security.auth.login.config=/etc/kafka/2.6.5.1050-37/0/kafka_client_jaas.conf
export JAVAOPTS="$KERBEROS -cp KafkaSample.jar:/usr/hdp/current/kafka-broker/libs/*  KafkaMain kafka.properties" 
```
**sh/kafka.properties** Uncomment:
```
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
```




