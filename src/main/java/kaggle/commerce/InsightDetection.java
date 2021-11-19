package kaggle.commerce;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/*
* Liệt kê 1 số insight từ tập dữ liệu
* */
public class InsightDetection {
    private void getInsight(SparkSession session){
        Dataset<Row> dsCustomer = session.read().option("header", "true")
                .csv("data/olist_customers_dataset.csv");
        Dataset<Row> dsSellers = session.read().option("header", "true")
                .csv("data/olist_sellers_dataset.csv");
        Dataset<Row> dsOrderReviews = session.read().option("header", "true")
                .csv("data/olist_order_reviews_dataset.csv");
        Dataset<Row> dsOrderItems = session.read().option("header", "true")
                .csv("data/olist_order_items_dataset.csv");
        Dataset<Row> dsProducts = session.read().option("header", "true")
                .csv("data/olist_products_dataset.csv");
        Dataset<Row> dsGeolocation = session.read().option("header", "true")
                .csv("data/olist_geolocation_dataset.csv");
        Dataset<Row> dsOrders = session.read().option("header", "true")
                .csv("data/olist_orders_dataset.csv");
        Dataset<Row> dsOrdersPayment = session.read().option("header", "true")
                .csv("data/olist_order_payments_dataset.csv");

        // Tỷ lệ % khách hàng ở các thành phố
        dsCustomer.createOrReplaceTempView("customers");
        String sql = "SELECT a.customer_city, count(a.customer_id)*100.0/max(b.total_customer) AS perc " +
                " FROM customers a, (SELECT count(customer_id) AS total_customer FROM customers) b " +
                " GROUP BY a.customer_city ORDER BY perc DESC";
        session.sql(sql).show(false);

        // Thống kê mỗi kiểu thanh toán có bao nhiêu bản ghi
        dsOrdersPayment.groupBy("payment_type").count()
                .sort(col("count").desc())
                .show(false);
    }
    public static void main(String[] args) {
        SparkSession session = SparkSession
                .builder()
                .appName("kaggle")
                .master("local[*]")
                .config("spark.testing.memory","471859200")
                .getOrCreate();

        InsightDetection insightDetection = new InsightDetection();
        insightDetection.getInsight(session);
    }
}
