package kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataUploadController {
    @Autowired
    private DataProducer dataProducer;

    @PostMapping(value="/streams",consumes = "application/json", produces = "text/plain")
    public ResponseEntity<String> dataUpload(@RequestBody String data) {
        ObjectMapper mapper = new ObjectMapper();
        String payloadData = null;

        try {
            payloadData = mapper.writeValueAsString(data);
            System.out.println("data: " + payloadData);
            // 12345 is partition key here. It is just an example.
            dataProducer.putIntoKinesis("shinray", "12345", payloadData
            );
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ResponseEntity.ok("Data uploaded to kinesis sucessfull");


    }
    @GetMapping("/hello")
    public String hello() {
        return "Hello";
    }
}
