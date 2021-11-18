package kaggle.commerce.crawler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/*
* Đọc dữ liệu từ kafka, filter và lưu xuống mysql hoặc hive dạng star join
* */
public class IngestionData {
	private static final String BOOTSTRAP_SERVER = "";
	private static final String TOPIC = "";
	private static final String GROUP_ID = "";

	public static void main(String[] args) {
		SparkSession session = SparkSession
				.builder()
				.appName("kaggle")
				.master("local[*]")
				.config("spark.testing.memory","471859200")
				.getOrCreate();

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVER);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		JavaStreamingContext stC = new JavaStreamingContext(sc, Durations.seconds(30));

		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						stC,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(Arrays.asList(TOPIC), kafkaParams)
				);

		stream.foreachRDD(rdd -> {
			double a = System.currentTimeMillis();
			Integer counts = rdd
					.mapToPair(f -> {
						f.value();
						return null;
					})
					.filter(f -> f != null)
					.flatMapToPair(f -> {
						return null;
					})
					.distinct()
					.mapPartitions(f -> {
						int count = 0;
						return Arrays.asList(count).iterator();
					}).reduce((x, y) -> x + y);

			System.out.println("Excute: " + counts);
			System.out.println("time run is: " + (System.currentTimeMillis() - a) / 1000);

		});
		stC.start();
		try {
			stC.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
