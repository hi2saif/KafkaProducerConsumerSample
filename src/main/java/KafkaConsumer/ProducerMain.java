package KafkaConsumer;


import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

class ProducerMain {
    private static final String TEST_TOPIC = "myTestTopic";
    public static final String broker = "127.0.0.1:9092";

    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop = setKafkaProp();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        String data = getJsonFromFile("data1.json");
        ProducerRecord<String, String> record  = new ProducerRecord<String, String>(TEST_TOPIC, new JSONObject(data).toString());
        try {
            producer.send(record);
        }catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
        System.out.println("send data : " + data);
    }


    private static String getJsonFromFile(String file) throws IOException {
        URL url = Resources.getResource(file);
        String json = Resources.toString(url, Charsets.UTF_8);
        return json;
    }
    public static Properties setKafkaProp() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleProducer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return prop;
    }
}
