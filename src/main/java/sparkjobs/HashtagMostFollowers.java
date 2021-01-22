package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.*;

public class HashtagMostFollowers extends SparkJob{

    public static Iterator<Tuple2<Tuple2<String, String>, Long>> extractHashtagUserAndNbFollowers(String line){
        List<Tuple2<Tuple2<String, String>, Long>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            if(user != null){
                Long nbFollowers = Long.valueOf(retrieveNbFollowers(json));
                if(nbFollowers > 0){
                    JSONArray hashtags = retrieveHashtags(json);
                    if(hashtags != null){
                        if(hashtags.length() > 0){ 
                            for(int i = 0; i < hashtags.length(); i++){
                                String hashtag = hashtags.getJSONObject(i).getString("text");
                                Tuple2<String, String> tupleHashtagUser = new Tuple2<String, String>(hashtag, user);
                                Tuple2<Tuple2<String, String>, Long> tupleHashtagUserNbFollowers = new Tuple2<Tuple2<String, String>, Long>(tupleHashtagUser, nbFollowers);
                                result.add(tupleHashtagUserNbFollowers);
                            }
                        }
                    }
                }
            }
        }
        return result.iterator();
    }
    
    public static void runJob(JavaSparkContext context, JavaRDD<String> data){

        List<Tuple2<String, Long>> test = data
            .flatMapToPair(line -> extractHashtagUserAndNbFollowers(line))
            .distinct()
            .mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2))
            .reduceByKey((a, b) -> a + b)
            .top(10, new TupleComparatorLong());


        JavaPairRDD<String, Long> test2 = context.parallelizePairs(test);
        System.out.println(test2.take(10));

    }
}