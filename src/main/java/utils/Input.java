package bigdata;

import java.io.Serializable;

public class Input<K,V> implements Serializable {

    private K key;
    private V myValue;

    public Input(K key, V myValue) {
        this.key = key;
        this.myValue = myValue;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getMyValue() {
        return myValue;
    }

    public void setMyValue(V myValue) {
        this.myValue = myValue;
    }
}