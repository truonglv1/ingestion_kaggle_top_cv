package kaggle.commerce.crawler;

import com.mongodb.spark.MongoSpark;
import kaggle.commerce.config.Constants;
import kaggle.commerce.database.MongoHandler;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import static org.apache.spark.sql.functions.col;

/*
* Đọc dữ liệu dataset bằng kaggle API và lưu xuống thư mục data
* Đẩy dữ liệu vào mongo và kafka
* */

public class KaggleCrawler implements Job {

    private void crawler(){
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        Request request = new Request.Builder()
                .url("https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce?datasetVersionNumber=2")
                .method("GET", null)
                .addHeader("Authorization", "Basic dHJ1b25nbHY5NjoyMmY0OWExMWZmYjdlNmI2MDE4ZjNmMTMzNGY0OWM0OQ==")
                .build();
        try {
            Response response = client.newCall(request).execute();

            InputStream in = response.body().byteStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String result, line = reader.readLine();
            result = line;
            while((line = reader.readLine()) != null) {
                result += line;
            }
            System.out.println(result);
            response.body().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readData(SparkSession session, String dir){
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    Dataset<Row> ds = session.read().option("header", "true").csv(path.toString());
                    if (path.getFileName().toString().equals("olist_orders_dataset.csv")){
                        MongoSpark.save(ds.limit(10).write().option("collection", "orders").mode("overwrite"));

                        session.udf().register("toJson", (String order_id, String customer_id, String order_status,
                                                          String order_purchase_timestamp, String order_approved_at,
                                                          String order_delivered_carrier_date,
                                                          String order_delivered_customer_date,
                                                          String order_estimated_delivery_date)-> {
                            JSONObject object = new JSONObject();
                            object.put("order_id", order_id);
                            object.put("customer_id", customer_id);
                            object.put("order_status", order_status);
                            object.put("order_purchase_timestamp", order_purchase_timestamp);
                            object.put("order_approved_at", order_approved_at);
                            object.put("order_delivered_carrier_date", order_delivered_carrier_date);
                            object.put("order_delivered_customer_date", order_delivered_customer_date);
                            object.put("order_estimated_delivery_date", order_estimated_delivery_date);
                            return object.toString();

                        }, DataTypes.StringType);

                        ds.limit(100)
                                .select(col("order_id").as("key"), callUDF("toJson",
                                        col("order_id"), col("customer_id"),
                                        col("order_status"),col("order_purchase_timestamp"),
                                        col("order_approved_at"),col("order_delivered_carrier_date"),
                                        col("order_delivered_customer_date"),col("order_estimated_delivery_date"))
                                        .as("value"))
                                .write()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", MongoHandler.host +":9092")
                                .option("num.replica.fetchers", "1")
                                .option("topic", Constants.ORDER_TOPIC)
                                .save();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        SparkSession session = SparkSession
                .builder()
                .appName("kaggle")
                .master("local[*]")
//                .config("spark.mongodb.output.uri","mongodb://"+ MongoHandler.userName +":"+MongoHandler.pass
//                        +"@"+MongoHandler.host+":"+MongoHandler.port+"/"+MongoHandler.dbName)
                .config("spark.testing.memory","471859200")
                .getOrCreate();

        KaggleCrawler kaggleCrawler = new KaggleCrawler();
        kaggleCrawler.readData(session, "data");
    }
}
