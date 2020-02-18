package kafka.SSE;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;

/**
 * SSE generaor
 */
public class SSERealRateGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    public SSERealRateGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.SSE.SSEPartitioner");
        producer = new KafkaProducer<String, String>(props);

    }

    public void generate() throws InterruptedException {

        int REPEAT = 10;

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;

        int end_count = 0;

        try {
            stream = new FileReader("/root/SSE-kafka-producer/partition1.txt");
            br = new BufferedReader(stream);

            start = System.currentTimeMillis();

            while ((sCurrentLine = br.readLine()) != null) {

                if (sCurrentLine.equals("end")) {
                    System.out.println("output rate: " + counter);
                    counter = 0;
                    end_count ++;
                    cur = System.currentTimeMillis();
                    if (cur-start < 1000) {
                        sleep(1000 - (cur - start));
                    }
                    start = System.currentTimeMillis();
                }

                if (sCurrentLine.split("\\|").length < 10) {
                    continue;
                }

//                if (end_count <= 10) {
//                    continue;
//                }

                for (int i=0; i< REPEAT; i++) {
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                    producer.send(newRecord);
                    counter++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
        //logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws InterruptedException {
        String TOPIC = new String("stock_sb");
        String file = new String("partition1");
        int speed = 1;
        if (args.length > 0) {
            TOPIC = args[0];
            file = args[1];
            speed = Integer.parseInt(args[2]);
        }
        new SSERealRateGenerator(TOPIC).generate();
    }
}

