package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import org.json.*;
import java.util.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;


import org.apache.hadoop.hbase.client.HBaseAdmin;
import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;


public abstract class SparkJob {

    protected static class TupleComparatorListString implements Comparator<Tuple2<List<String>, Long>>, Serializable {

        @Override
        public int compare(Tuple2<List<String>, Long> tuple1, Tuple2<List<String>, Long> tuple2) {
            return Long.compare(tuple1._2, tuple2._2);
        }
    }

    protected static class TupleComparatorString implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
            return Long.compare(tuple1._2, tuple2._2);
        }
    }

    protected static class TupleComparatorStringInt implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
        }
    }

    protected static class TupleComparatorLong implements Comparator<Tuple2<String, Long>>, Serializable {

        @Override
        public int compare(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
            return Long.compare(tuple1._2, tuple2._2);
        }
    }

    protected static JSONArray retrieveHashtags(JSONObject json){
        JSONObject entities = null;
        try {
            entities = json.getJSONObject("entities");
        }catch(Exception e) { }

        if(entities != null){
            JSONArray hashtags = null;
            try {
                hashtags = entities.getJSONArray("hashtags");
            }catch(Exception e) { }

            if(hashtags != null){
                return hashtags;
            }
        }
        return null;       
    }

    public static String retrieveUser(JSONObject json){
        JSONObject user = null;
        try {
            user = json.getJSONObject("user");
        }catch(Exception e) { }

        if(user != null){
            String userInfo = user.getString("name");
            return userInfo;
        }
        return null;
    }

    public static int retrieveNbFollowers(JSONObject json){
        JSONObject user = null;
        try {
            user = json.getJSONObject("user");
        }catch(Exception e) { }

        if(user != null){
            Integer nbFollowers = user.getInt("followers_count");
            return nbFollowers; 
        }
        return -1;
    }

    public static int retrieveNbRetweets(JSONObject json){
        int nbReweets = -1;
        try {
            nbReweets = json.getInt("retweet_count");
        }catch(Exception e) { }

        return nbReweets;
    }

    public static Long retrieveTweetId(JSONObject json){
        long tweetId = (long) -1;
        try {
            tweetId = json.getLong("id");
        }catch(Exception e) { }

        return tweetId;
    }
}
