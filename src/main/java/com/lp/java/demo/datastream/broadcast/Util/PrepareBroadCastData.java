package com.lp.java.demo.datastream.broadcast.Util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PrepareBroadCastData {
    public static List<Tuple2<String,String>> getBroadcastData(){
        List<Tuple2<String,String>> data = new ArrayList<>();

        data.add(new Tuple2<>("apple","red"));
        data.add(new Tuple2<>("pear","white"));
        data.add(new Tuple2<>("nut","black"));
        data.add(new Tuple2<>("grape","orange"));
        data.add(new Tuple2<>("banana","yellow"));
        data.add(new Tuple2<>("pineapple","purple"));
        data.add(new Tuple2<>("pomelo","blue"));
        data.add(new Tuple2<>("orange","ching"));
        return data;
    }
}
