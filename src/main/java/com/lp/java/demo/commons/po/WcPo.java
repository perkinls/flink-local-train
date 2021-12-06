package com.lp.java.demo.commons.po;

/**
 * Simple POJO containing a word and its respective count.
 */
public class WcPo {
    public String word;
    public long frequency;

    // public constructor to make it a Flink POJO
    public WcPo() {
    }

    public WcPo(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WC " + word + " " + frequency;
    }
}
