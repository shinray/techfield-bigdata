import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class asynchronous_producer {
    public static void main(String[] args) throws Exception {

        // define a kafka topic
        String topicName = "SynchronousProducer";

        // key/value
        String key = "key-1";
        String value = "value-1";

        // kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092,localhost:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // instantiate producer, create a quick record
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key,value);

//        try {
//            RecordMetadata metadata = producer.send(record).get();
//            System.out.println("Message sent to partition no: " + metadata.partition() + ", offset no: "
//                    + metadata.offset());
//            System.out.println("SynchronousProducer completed successfully");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("SynchronousProducer crashed with exception.");
//        } finally {
//            producer.close(); // Always close producer
//        }

        producer.send(record, new MyProducerCallback());
        System.out.println("AsynchronousProducer call completed");
        producer.close();
    }
}

class MyProducerCallback implements Callback {

    //@Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsynchronousProducer failed with exception");
        else
            System.out.println("AsynchronousProducer callback success");
    }
}