import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class SupplierDeserializer implements Deserializer<Supplier> {
    private String encoding = "UTF8";

//    @Override
    public void configure(Map<String,?> configs, boolean isKey) {}

//    @Override
    public Supplier deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                System.out.println("supplierDeserializer.deserialize received null");
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);
            int id = buf.getInt();

            int sizeOfName = buf.getInt();
            byte[] nameBytes = new byte[sizeOfName];
            buf.get(nameBytes);
            String deserializedName = new String(nameBytes, encoding);

            int sizeOfDate = buf.getInt();
            byte[] dateBytes = new byte[sizeOfDate];
            buf.get(dateBytes);
            String dateString = new String(dateBytes,encoding);

            DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

            return new Supplier(id,deserializedName,df.parse(dateString));

        } catch (Exception e) {
            throw new SerializationException("Error when deserializing data into a Supplier", e);
        }
    }

//    @Override
    public void close() {}
}
