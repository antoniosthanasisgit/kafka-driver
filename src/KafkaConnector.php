<?php

namespace Kafka;

trait KafkaConnector {

    public function connect() {

        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', env('BOOTSTRAP_SERVERS'));
        $conf->set('security.protocol', env('SECURITY_PROTOCOL', 'SASL_SSL'));
        $conf->set('sasl.mechanisms', env('SASL_MECHANISMS', 'PLAIN'));
        $conf->set('sasl.username', env('SASL_USERNAME'));
        $conf->set('sasl.password', env('SASL_PASSWORD'));

        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', env('GROUP_ID', 'group'));
        $conf->set('auto.offset.reset', env('AUTO_OFFSET_RESET','earliest'));

        $consumer = new \RdKafka\KafkaConsumer($conf);

        return [
            'producer' => $producer,
            'consumer' => $consumer
        ];

    }
}

