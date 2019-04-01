import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class supplier_consumer {
    public static void main(String[] args) throws Exception {

        // define topic and group
        String topicName = "SupplierTopic";
        String groupName = "SupplierTopicGroup";

        // define consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092,localhost:9093");
        props.put("group.id",groupName); // important
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","SupplierDeserializer");

        InputStream input = null;

        // Instantiate consumer
        KafkaConsumer<String, Supplier> consumer = null;

        try {
            consumer = new KafkaConsumer<String, Supplier>(props);
            consumer.subscribe(Arrays.asList(topicName));
//            input = new FileInputStream()
            while (true) {
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String,Supplier> record : records) {
                    System.out.println("Supplier id: " + String.valueOf(record.value().getID())
                    + " Supplier Name = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("supplierconsumer exception");
        } finally {
            consumer.close();
        }
    }
}
