package twitterProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
//import com.twitter.hbc.httpclient.auth.Authentication;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.*;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// twitterProducer_Z7YPAR8

public class twitterProducer {

    // twitter super sekrit keys shh don't tell
    private static final String CONSUMER_KEY = "WXC50jvoWct4SzHLHHeHI7MzD";
    private static final String CONSUMER_SECRET = "pDA0JMsoka4RUGnKzwOKJBWxNN98JopCvOc6ymom4u1Ac1DnyP";
    // Access level: READ_ONLY
    private static final String ACCESS_TOKEN = "1105476295713087488-W3nJ7Fmty1TekFcFOTWlYr5JB0lxXN";
    private static final String ACCESS_SECRET = "dXW78QUoHa25WLHWPw2LryU9VCasuXirqIGQxTDzLV0Hc";

    // Some stuff for Kafka
//    private static final String kafkaTopic = "twitterproducer";
    private static final String kafkaTopic = "kafkaConsumerSpark";
    private static final String kafkaServer = "localhost:9092";

    // Keywords to look for
    private static final String[] keywords = {"article13","usavchi","travel"};

    public static void run(String consumerKey, String consumerSecret,
                           String token, String secret) throws InterruptedException{
        // Producer settings
        Properties props = new Properties();
        props.put("bootstrap.servers",kafkaServer); // TODO: add more?
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // Create producer
        Producer<String,String> producer = new KafkaProducer<>(props);


        // Twitter stuff
        BlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(10000);

        //Authentication
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(token)
                .setOAuthAccessTokenSecret(secret);

        TwitterStream ts = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);

                System.out.println("@" + status.getUser().getScreenName() + " : " + status.getText());
                for (URLEntity entity : status.getURLEntities()) {
                    System.out.println(entity.getDisplayURL());
                }
                for (HashtagEntity entity : status.getHashtagEntities()) {
                    System.out.println(entity.getText());
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Status deletion - id: " + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Track limitation - id: " + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Scrubgeo - userId: " + userId + " , upToStatusId: " + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Stall warning: " + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        ts.addListener(listener);
        FilterQuery query = new FilterQuery().track(keywords);
        ts.filter(query);

        // Main loop. idle counter kills program if idle for a total of 10sec
        int i = 0;
        while(i < 100) {
            Status msg = queue.poll();
            if (msg == null) {
                Thread.sleep(100);
                i++;
            } else {
                for (HashtagEntity hashtag: msg.getHashtagEntities()) {
                    System.out.println("Hashtag: " + hashtag.getText());
                    // If a valid partition number is specified that partition will be used when sending the record.
                    // If no partition is specified but a key is present a partition will be chosen using a hash of the key.
                    // If neither key nor partition is present a partition will be assigned in a round-robin fashion.
                    producer.send(new ProducerRecord<String, String>(
                            kafkaTopic,hashtag.getText()
                    ));
                }
                // Reset idle
                i = 0;
            }
        }

        // cleanup
        producer.close();
        Thread.sleep(5000);
        //ts.cleanUp();
        ts.shutdown();
    }

    public static void main(String[] args) {
        System.out.println("twitter for kafka producer \uD83D\uDC4C");

        try {
            twitterProducer.run(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        System.out.println("Thanks for playing! Goodbye ❤");
    }
}
