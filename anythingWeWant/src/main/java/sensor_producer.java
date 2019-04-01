import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class sensor_producer {

    public static void main(String[] args) throws Exception {

        String topicName = "Sensor10";

        // Properties
        Properties props = new Properties();
        // Basic
        props.put("bootstrap.servers","localhost:9092,localhost:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // Partitioner
        props.put("partitioner.class","sensor_partitioner");
        props.put("speed.sensor.name","TSS");

        // Instantiate producer
        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,"SSP"+i,"500"+i));
        }
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,"TSS","500"+i));
        }

        // Cleanup
        producer.close();
        System.out.println("Sensor producer completed");
    }

}
