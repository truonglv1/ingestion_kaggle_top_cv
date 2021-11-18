package kaggle.commerce.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadConfig {
    private static final Logger LOG = LogManager.getLogger(LoadConfig.class);

    private static Properties properties = null;

    static {
        try {
            InputStream stream = LoadConfig.class
                    .getResourceAsStream("/kaggle.properties");
            properties = new Properties();
            properties.load(stream);
            stream.close();
        } catch (IOException e) {
            LOG.error(e.getCause().getMessage(), e);
        }

    }

    public static Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        String a = LoadConfig.getProperties().getProperty("MONGO_HOST");
        System.out.println(a);
    }
}
