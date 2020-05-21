package samzatask.wc;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.io.Serializable;

public class WordCountSplitterTask implements StreamTask, Serializable {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "words");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String sentence = (String) envelope.getMessage();
        String[] tokens = sentence.toLowerCase().split("\\W+");
        for (String token : tokens) collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, token, token));
    }
}
