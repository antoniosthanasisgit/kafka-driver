<?php

namespace Kafka;

use Kafka\KafkaConnector;

trait Producer {

    use KafkaConnector;

    public function produceMessage($topic, $message) {
        if (env('APP_ENV') === 'production') {

            $producer = $this->connect()['producer'];

            $topic = $producer->newTopic($topic);

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($message));

            $producer->flush(1000);
        }
    }


}
