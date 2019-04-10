package kinesis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication//(scanBasePackageClasses = {DataUploadController.class})
//(scanBasePackages = {"kinesis_producer"})
//scanBasePackages = {"com.shinray.kinesis_producer","controller"}
//@ComponentScan({"com.shinray.kinesis_producer","kinesis.DataUploadController"})
//@EntityScan("com.shinray.kinesis_producer.controller")
//public class KinesisProducerApplication extends SpringBootServletInitializer {
//
//	@Override
//	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
//		return application.sources(KinesisProducerApplication.class);
//	}
//
//	public static void main(String[] args) {
//		SpringApplication.run(KinesisProducerApplication.class, args);
//	}
//
//}
public class KinesisProducerApplication {
	public static void main(String[] args) {
		SpringApplication.run(KinesisProducerApplication.class, args);
	}
}
