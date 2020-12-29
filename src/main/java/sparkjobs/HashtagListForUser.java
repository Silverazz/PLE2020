package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class HashtagListForUser extends SparkJob{

    public static Iterator<Tuple2<String, String>> extractTupleUserHashtagFromLine(String line){
        List<Tuple2<String, String>> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            String user = retrieveUser(json);
            if(hashtags != null && user != null){
                for(int i = 0; i < hashtags.length(); i++){
                    String hashtag = hashtags.getJSONObject(i).getString("text");
                    Tuple2<String, String> tuple = new Tuple2<String, String>(user, hashtag);
                    result.add(tuple);
                }
            }
        }

        return result.iterator();
    }

    public static void runJob(JavaSparkContext context, JavaRDD<String> data){
        JavaPairRDD <String, Iterable<String>> test = data
            .flatMapToPair( line -> extractTupleUserHashtagFromLine(line))
            .distinct()
            .groupByKey();

        System.out.println(test.take(100));
    }
}

