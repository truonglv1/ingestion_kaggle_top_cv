package kaggle.commerce.database;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import kaggle.commerce.config.LoadConfig;
import org.bson.Document;
import java.util.ArrayList;

public class MongoHandler {
    public MongoClient mongoClient;
    public MongoCredential mongoCredential;
    public MongoDatabase database;
    public MongoClientOptions options;
    public static String userName = LoadConfig.getProperties().getProperty("MONGO_USER");
    public static String dbName = LoadConfig.getProperties().getProperty("MONGO_DB");
    public static String host = LoadConfig.getProperties().getProperty("MONGO_HOST");
    public static int port = Integer.parseInt(LoadConfig.getProperties().getProperty("MONGO_PORT"));
    public static String pass = LoadConfig.getProperties().getProperty("MONGO_PASS");

    public MongoHandler(){
        this.mongoCredential = MongoCredential.createCredential(userName, dbName, pass.toCharArray());
        this.options = MongoClientOptions.builder().sslEnabled(false).build();
        this.mongoClient = new MongoClient(new ServerAddress(host, port), mongoCredential, options);
        this.database = mongoClient.getDatabase(dbName);
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public void close(){
        mongoClient.close();
    }

    // read documents
    public ArrayList<Document> getAllDocument(int page, int offset, String collectionName){
        ArrayList<Document> documents = new ArrayList<>();
        MongoCollection<Document> articleCollection = database.getCollection(collectionName);

        documents = articleCollection.find()
                .skip( page > 0 ? ( ( page - 1 ) * offset ) : 0 )
                .limit(offset)
                .into(new ArrayList<>());

        return documents;
    }

    // insert document
    public void insertDocument(Document document, String collection){
        database.getCollection(collection).insertOne(document);
    }
    public static void main(String[] args) {
        MongoHandler mongoHandler = new MongoHandler();
        mongoHandler.getAllDocument(1, 10, "sellers")
                .forEach(System.out::println);
    }
}
