import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaMain {

	private static final String LIST = "list";
	private static final String PRODUCE = "produce";
	private static final String TOPIC = "topic";
	private static final String CONSUME = "consume";

	private static void listTopics(Properties props) {
		Map<String, List<PartitionInfo>> topics;
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		P("I'm creating KafkaConsumer.");
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
			P("Created, now pull the topic list.");
			topics = consumer.listTopics();
			P("Print list of topics received. Number of topics: " + topics.size());
		}
		topics.forEach((k, v) -> System.out.println("Topic : " + k + " PartitionInfo : " + v));
		P("End of list.");
	}

	private static void produceLines(Properties props, String topic) throws InterruptedException, ExecutionException {
		P("I'm producing lines to topic " + topic + " unless stopped by CTRL/C ...");
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

	private static void consumeLines(Properties props, String topic) throws InterruptedException, ExecutionException {
		P("I'm consuming lines from topic " + topic + " unless stopped by CTRL/C ...");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		try (Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singletonList(topic));
			while (true) {
				ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
				consumerRecords.forEach(record -> {
					System.out.println(record.key() + " " + record.value());
				});
			}
		}
	}

// ============================================================
// read by partitions, more complicated
// allows moving offset to the end and ignore old messages
// ============================================================

	static class ReadPartition implements Callable<Void> {

		
		private final TopicPartition pinfo;
		private final Properties prop;
		
		ReadPartition(Properties prop,String topic,int partitioninfo) {
			pinfo = new TopicPartition(topic,partitioninfo);
			this.prop = prop;
		}
		
		@Override
		public Void call() throws Exception {
			try (Consumer<Long, String> consumer = new KafkaConsumer<>(prop)) {
				consumer.assign(Arrays.asList(pinfo));
				consumer.seekToEnd(Arrays.asList(pinfo));
				System.out.println("Read for partition " + pinfo.partition());
				while (true) {
					ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
					consumerRecords.forEach(record -> {
						System.out.println(pinfo.partition() + "   " + record.key() + " " + record.value());
					});
				}
			}			
		}

	}
	
	private static void consumeLinesByPartitions(Properties props, String topic)
			throws InterruptedException, ExecutionException {
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		P("I'm consuming lines from topic " + topic + " unless stopped by CTRL/C ...");
		Collection<PartitionInfo> pList;
		try (Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
			pList = consumer.partitionsFor(topic);
		}
		ExecutorService e = Executors.newCachedThreadPool();
		pList.forEach(t -> e.submit(new ReadPartition(props,topic,t.partition())));
		e.shutdown();
		e.awaitTermination(30,TimeUnit.MINUTES);		
	}
	
	// ======================
	// another version
	// ======================

	private static void consumeLinesByPartitonsPrevious(Properties props, String topic)
			throws InterruptedException, ExecutionException {
		P("I'm consuming lines from topic " + topic + " unless stopped by CTRL/C ...");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		try (Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
			Collection<PartitionInfo> tlist = consumer.partitionsFor(topic);
			Collection<TopicPartition> plist = tlist.stream().map(t -> new TopicPartition(topic, t.partition()))
					.collect(Collectors.toList());
			consumer.assign(plist);
			consumer.seekToEnd(plist);
			while (true) {
				ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
				consumerRecords.forEach(record -> {
					System.out.println(record.key() + " " + record.value());
				});
			}
		}
	}

	private static void P(String s) {
		System.out.println(s);
	}

	private static void printHelp() {
		P("Parameters: <config file> <action>");
		P("  config file : path to property file");
		P("  action: " + LIST + ", " + PRODUCE + " ,and " + CONSUME);
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
		else if (CONSUME.equals(args[1]))
//			consumeLines(props, topic);
			consumeLinesByPartitions(props,topic);
		else
			printHelp();
	}

}
