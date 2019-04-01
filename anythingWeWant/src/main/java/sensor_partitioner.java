import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class sensor_partitioner implements Partitioner {
    private String speedSensorName;

//    @Override
    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

//    @Override
    public int partition(String topic, Object objectKey, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int sp = (int)Math.abs(numPartitions*0.3);
        int p = 0;

        if ((keyBytes == null) || (!(objectKey instanceof String)))
            throw new InvalidRecordException("All messages must have sensor name as key");

        if (((String)objectKey).equals(speedSensorName))
            p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
        else
            p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;

        System.out.println("key = " + objectKey + ", Partition = " + p);
        return p;
    }

//    @Override
    public void close() {

    }
}
