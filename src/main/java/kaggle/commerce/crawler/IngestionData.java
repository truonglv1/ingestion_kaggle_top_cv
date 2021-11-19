package kaggle.commerce.crawler;

import kaggle.commerce.config.Constants;
import kaggle.commerce.database.MongoHandler;
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
import org.bson.Document;
import org.json.JSONObject;
import scala.Tuple2;
import java.util.*;

/*
* Đọc dữ liệu từ kafka, filter và lưu xuống mysql hoặc hive dạng star join
* */
public class IngestionData {
	private static final String BOOTSTRAP_SERVER = MongoHandler.host+":9092";
	private static final String TOPIC = Constants.ORDER_TOPIC;
	private static final String GROUP_ID = "kaggle";

	public static void main(String[] args) {
		SparkSession session = SparkSession
				.builder()
				.appName("kaggle ingestion")
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
			rdd.mapToPair(f -> {
				JSONObject object = new JSONObject(f.value());
				if (object!= null){
					return new Tuple2<>(object.getString("id"),f.value());
				}
				return null;
			}).filter(f->f!= null)
					.mapPartitions(f->{
						while (f.hasNext()){
							MongoHandler mongoHandler = new MongoHandler();
							JSONObject object = new JSONObject(f.next()._2);
							Document document = new Document();
							document.append("order_id", f.next()._1);
							document.append("customer_id", object.get("customer_id"));
							document.append("order_status", object.get("order_status"));
							mongoHandler.insertDocument(document,"orders");
						}
						return null;
					});

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
