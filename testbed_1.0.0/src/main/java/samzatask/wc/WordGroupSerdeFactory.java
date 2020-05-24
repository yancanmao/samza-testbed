package samzatask.wc;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class WordGroupSerdeFactory implements SerdeFactory<HashMap<String, Integer>> {
    public WordGroupSerdeFactory() {
    }

    public Serde<HashMap<String, Integer>> getSerde(String name, Config config) {
        return new WordGroupSerde();
    }
}
