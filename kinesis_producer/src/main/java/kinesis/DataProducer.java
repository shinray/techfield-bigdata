package kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Component("dataProducer")
public class DataProducer {
    private static final Logger logger = LoggerFactory.getLogger(DataProducer.class);

    @Value(value = "${aws_stream_name}")
    private String awsStreamName;
    @Value(value = "${aws_access_key}")
    private String awsAccessKey;
    @Value(value = "${aws_secret_key}")
    private String awsSecretKey;
    @Value(value = "${aws_region}")
    private String awsRegion;

    private KinesisProducer kinesisProducer = null;

    private final AtomicLong recordPut = new AtomicLong(0);
    private static final Random RANDOM = new Random();

    //

    private KinesisProducer getKinesisProducer() {

        if (kinesisProducer == null) {

            KinesisProducerConfiguration config = new KinesisProducerConfiguration();
            config.setRegion(awsRegion);
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            config.setCredentialsProvider(new AWSStaticCredentialsProvider(awsCreds));
            config.setMaxConnections(1);
            config.setRequestTimeout(6000); // 6 seconds
            config.setRecordMaxBufferedTime(5000); // 5 seconds
            kinesisProducer = new KinesisProducer(config);
        }

        return kinesisProducer;
    }

    public void putIntoKinesis(String streamName, String partitionKey, String payload) throws Exception {
        kinesisProducer = getKinesisProducer();
        if (partitionKey == null || partitionKey.isEmpty() || payload == null | payload.isEmpty()) {
            return;
        }

        ByteBuffer data = ByteBuffer.wrap(payload.getBytes("UTF-8"));

        while (kinesisProducer.getOutstandingRecordsCount() > 0.5) {
            Thread.sleep(1);
        }

        recordPut.incrementAndGet();

        ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(streamName, partitionKey, data);

        Futures.addCallback(f, new FutureCallback<UserRecordResult>() {

            @Override
            public void onSuccess(UserRecordResult result) {
                logger.info("Successfully put data into kinesis");

            }

            @Override
            public void onFailure(Throwable t) {
                logger.info("Failed to put data into kinesis");

                if (t instanceof UserRecordFailedException) {
                    UserRecordFailedException e = (UserRecordFailedException) t;
                    UserRecordResult result = e.getResult();
                    logger.info("Result {} ", result.isSuccessful());
                }
            }
        });
    }

    public void stop(){
        if (kinesisProducer != null) {
            kinesisProducer.flushSync();
            kinesisProducer.destroy();
        }
    }


}
