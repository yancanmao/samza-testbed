package kafka.WordCount;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * SSE generaor
 */
public class SentenceGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    /** how many sentences to output per second **/
    private int rate;
//    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize = 100;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;

    public SentenceGenerator(String input, String BROKERS, int rate, int cycle, int base) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        generator = new RandomSentenceGenerator();
        TOPIC = input;
        this.rate = rate;
        this.cycle = cycle;
        this.base = base;

        producer = new KafkaProducer<>(props);
    }

    public void generate() throws InterruptedException {
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(10000);

        long startTs = System.currentTimeMillis();

        while (running) {
            long emitStartTime = System.currentTimeMillis();
            if (System.currentTimeMillis() - startTs < warmUpInterval) {
                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, generator.nextSentence(sentenceSize));
                    producer.send(newRecord);
                }
                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
            } else {
                if (count == 20) {
                    // change input rate every 1 second.
//                    epoch++;
                    epoch = (int) ((emitStartTime - startTs - warmUpInterval) / 1000);
                    curRate = base + Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, generator.nextSentence(sentenceSize));
                    producer.send(newRecord);
                }
                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
                count++;
            }
        }
        producer.close();
    }

    private int changeRate(int epoch) {
        double sineValue = Math.sin(Math.toRadians(epoch*360/cycle)) + 1;
        System.out.println(sineValue);

        Double curRate = (sineValue * rate);
        return curRate.intValue();
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String BROKERS = params.get("host", "localhost:9092");
        String TOPIC = params.get("topic", "sentences");
        int rate = params.getInt("rate", 1000);
        int cycle = params.getInt("cycle", 360);
        int base = params.getInt("base", 0);

        new SentenceGenerator(TOPIC, BROKERS, rate, cycle, base).generate();
    }
}

