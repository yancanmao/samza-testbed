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

    private KeyValueStore<String, HashMap<String, Integer>> wordCountGroups;

    @Override
    public void init(Context context) throws Exception {
        this.wordCountGroups = (KeyValueStore<String, HashMap<String, Integer>>)
                context.getTaskContext().getStore("word-count-groups");

        System.out.println("+++++Store loaded successfully!");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String word = (String) envelope.getMessage();

        delay();

        String key = firstTwo(word);
        if (this.wordCountGroups.get(key) == null) {
            this.wordCountGroups.put(key, new HashMap<>());
        }
        HashMap<String, Integer> curCountGroup = this.wordCountGroups.get(key);
        int curCount = curCountGroup.getOrDefault(word, 0);
        curCount++;
        curCountGroup.put(word, curCount);
        this.wordCountGroups.put(key, curCountGroup);
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, word, String.valueOf(curCount)));
    }

    private void delay() {
        Long start = System.nanoTime();
        while (System.nanoTime() - start < 65000) {}
    }

    public String firstTwo(String str) {
        return str.length() < 2 ? str : str.substring(0, 2);
    }
}
