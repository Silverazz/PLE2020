package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import org.json.*;
import java.util.*;

public abstract class SparkJob {

    protected static class TupleComparatorListString implements Comparator<Tuple2<List<String>, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<List<String>, Integer> tuple1, Tuple2<List<String>, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
        }
    }

    protected static class TupleComparatorString implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
        }
    }

    protected static class TupleComparatorListStringAndString implements Comparator<Tuple2<Tuple2<List<String>, String>, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Tuple2<List<String>, String>, Integer> tuple1, Tuple2<Tuple2<List<String>, String>, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
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
}
