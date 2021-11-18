package kaggle.commerce.crawler;

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
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

/*
* Đọc dữ liệu dataset bằng kaggle API và lưu xuống thư mục data
* Đẩy dữ liệu vào mongo và kafka
* */

public class KaggleCrawler {

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

    private void readData(SparkSession spark, String dir){
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    System.out.println(path.getFileName().toString());
                    Dataset<Row> df = spark.read().option("header", "true").csv(path.toString());
                    if (path.getFileName().toString().equals("olist_sellers_dataset.csv")){
//                        MongoSpark.save(df.write().option("collection", "sellers").mode("overwrite"));
                        StructType schema = new StructType()
                                .add("seller_id",StringType)
                                .add("seller_zip_code_prefix",StringType)
                                .add("seller_city",StringType)
                                .add("seller_state",StringType);

                        df.select(col("seller_id").as("key"), col("seller_zip_code_prefix").as("value"))
                                .write()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", MongoHandler.host +":9092")
                                .option("topic", "kaggle")
                                .save();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("kaggle")
                .master("local[*]")
//                .config("spark.mongodb.output.uri","mongodb://"+ MongoHandler.userName +":"+MongoHandler.pass
//                        +"@"+MongoHandler.host+":"+MongoHandler.port+"/"+MongoHandler.dbName)
                .config("spark.testing.memory","471859200")
                .getOrCreate();

        KaggleCrawler kaggleCrawler = new KaggleCrawler();
        kaggleCrawler.readData(spark, "data");
    }
}
