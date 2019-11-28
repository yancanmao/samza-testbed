package samzaapps.Nexmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.*;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import samzaapps.Nexmark.serde.Auction;
import samzaapps.Nexmark.serde.Bid;
import samzaapps.Nexmark.serde.Person;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Query11 implements StreamApplication {

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String BID_STREAM = "bids";
    private static final String OUTPUT_STREAM_ID = "results";

    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        StringSerde stringSerde = new StringSerde();
        JsonSerdeV2<Person> personSerde = new JsonSerdeV2<>(Person.class);
        JsonSerdeV2<Bid> bidSerde = new JsonSerdeV2<>(Bid.class);
        JsonSerdeV2<Auction> auctionSerde = new JsonSerdeV2<>(Auction.class);

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<Bid> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(BID_STREAM,
                        bidSerde);


        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

        MessageStream<Bid> bids = appDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> results = appDescriptor.getOutputStream(outputDescriptor);

        MessageStream<Bid> repartitionedBids =
                bids
                        .partitionBy(bd -> String.valueOf(bd.getBidder()), bd -> bd, KVSerde.of(stringSerde, bidSerde), "auction")
                        .map(KV::getValue);

        repartitionedBids
                .window(Windows.keyedSessionWindow(
                    bd-> String.valueOf(bd.getBidder()), Duration.ofSeconds(60),
                    ()-> 0, (m, prevCount) -> prevCount + 1,
                    new StringSerde(), new IntegerSerde()), "count"
                ) // TODO: check which window is more suitable here
                .map(windowPane -> {
                    String auctionId = windowPane.getKey().getKey();
                    int count = windowPane.getMessage();
                    return KV.of(auctionId, auctionId + ": " + count);
                })
                .sendTo(results);
    }
}
