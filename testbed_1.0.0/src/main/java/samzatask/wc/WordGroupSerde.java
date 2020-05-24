package samzatask.wc;

import org.apache.samza.serializers.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class WordGroupSerde implements Serde<HashMap<String, Integer>> {
    public WordGroupSerde() {
    }

    @Override
    public HashMap<String, Integer> fromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bias);
            HashMap<String, Integer> wordGroup;
            wordGroup = (HashMap<String, Integer>) ois.readObject();
            return wordGroup;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(HashMap<String, Integer> wordGroup) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream dos = new ObjectOutputStream(baos);
            dos.writeObject(wordGroup);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


