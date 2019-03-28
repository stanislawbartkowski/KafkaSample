import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaMain {

	private static final String LIST = "list";
	private static final String PRODUCE = "produce";
	private static final String TOPIC = "topic";

	private static void listTopics(Properties props) {
		Map<String, List<PartitionInfo>> topics;
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
			topics = consumer.listTopics();
		}
		topics.forEach((k, v) -> System.out.println("Topic : " + k + " PartitionInfo : " + v));
	}

	private static void produceLines(Properties props, String topic) throws InterruptedException, ExecutionException {
		P("I'm producing lines unless stopped by CTRL/C ...");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		long key = 0l;
		while (true) {
			try (KafkaProducer<Long, String> producer = new KafkaProducer<>(props)) {
				if (key % 100 == 0)
					P("Key:" + key);
				ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, "Line number: " + key);
				key++;
				producer.send(record).get();
			}
		}
	}

	private static void P(String s) {
		System.out.println(s);
	}

	private static void printHelp() {
		P("Parameters: <config file> <action>");
		P("  config file : path to property file");
		P("  action: " + LIST + ", " + PRODUCE + ", consume");
		P("Example:");
		P(" ./kafka.properties " + LIST);
		P(" ./kafka.properties " + PRODUCE);
		System.exit(4);
	}

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		if (args.length != 2)
			printHelp();
		Properties props = new Properties();
		try (InputStream input = new FileInputStream(args[0])) {
			props.load(input);
		}
		String topic = props.getProperty(TOPIC);
		if (LIST.equals(args[1]))
			listTopics(props);
		else if (PRODUCE.equals(args[1]))
			produceLines(props, topic);
		else
			printHelp();
	}

}