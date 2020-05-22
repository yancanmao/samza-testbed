package samzatask.wc;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class WordCountCounterTask implements StreamTask, InitableTask, Serializable {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "counts");

    private KeyValueStore<String, Integer> wordcounts;

    @Override
    public void init(Context context) throws Exception {
        this.wordcounts = (KeyValueStore<String, Integer>) context.getTaskContext().getStore("word-counts");
        System.out.println("+++++Store loaded successfully!");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String word = (String) envelope.getMessage();
        delay();
        if (this.wordcounts.get(word) == null) {
            this.wordcounts.put(word, 0);
        }
        int curCount = this.wordcounts.get(word);
        curCount++;
        this.wordcounts.put(word, curCount);
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, word, String.valueOf(curCount)));
    }

    private void delay() {
        Long start = System.nanoTime();
        while (System.nanoTime() - start < 12500) {}
    }
}
