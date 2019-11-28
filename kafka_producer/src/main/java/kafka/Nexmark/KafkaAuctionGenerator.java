package kafka.Nexmark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.beam.sdk.nexmark.model.Auction;
import java.util.Properties;
import java.util.Random;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * SSE generaor
 */
public class KafkaAuctionGenerator {

    private String TOPIC;

    private static KafkaProducer<Long, String> producer;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private volatile boolean running = true;
    private long eventsCountSoFar = 0;

    public KafkaAuctionGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class", "generator.SSEPartitioner");
        producer = new KafkaProducer<Long, String>(props);
    }

    public void generate(int rate) throws InterruptedException {

        while (running && eventsCountSoFar < 20_000_000) {
            long emitStartTime = System.currentTimeMillis();

            for (int i = 0; i < rate; i++) {

                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

                System.out.println(AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config).toString());

//                ProducerRecord<Long, Auction> newRecord = new ProducerRecord<Long, Auction>(TOPIC, nextId,
//                        AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config));
                ProducerRecord<Long, String> newRecord = new ProducerRecord<Long, String>(TOPIC, nextId,
                        AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config).toString());
                producer.send(newRecord);
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }

        producer.close();
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }


    public static void main(String[] args) throws InterruptedException {
        String TOPIC = new String("auction");
        int rate = 1;
        if (args.length > 0) {
            TOPIC = args[0];
            rate = Integer.parseInt(args[2]);
        }
        new KafkaAuctionGenerator(TOPIC).generate(rate);
    }
}

