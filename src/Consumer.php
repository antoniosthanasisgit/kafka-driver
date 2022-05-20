<?php

namespace Kafka;

use Illuminate\Console\Command;
use Kafka\KafkaConnector;

class Consumer extends Command
{
    use KafkaConnector;
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'consume:topic_messages';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $consumer = $this->connect()['consumer'];

        /**
         *  returns and array of topics that containts
         *  as a key the name of the topic in your apache kafka
         *  and as value the job.
         * 
         *  return [topics => ['topic_name' => [ App\Jobs\MyJob ]]
         */

        $topics = config('topics.topics');

        $consumer->subscribe(array_keys($topics));

        while (true) {
            $message = $consumer->consume(120 * 1000);

            if (isset($message->topic_name)) {
                $liseners = $topics[$message->topic_name];

                foreach ($liseners as $lisener) {
                    if (!empty($message->payload)) {
                        $class = new $lisener(json_decode($message->payload));
                        dispatch($class);
                    }
                }
            }
        }
    }
}
