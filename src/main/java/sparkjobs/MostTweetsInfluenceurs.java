package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.*;

public class MostTweetsInfluenceurs extends SparkJob{

    public static Iterator<List<String>> extractHashtagTriplets(String line){
        List<List<String>> result = new ArrayList();
        List<String> hashtagsList = new ArrayList();  

        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                if(hashtags.length() == 3){ 
                    for(int i = 0; i < hashtags.length(); i++){
                        hashtagsList.add(hashtags.getJSONObject(i).getString("text"));
                    }
                    hashtagsList.sort(Comparator.comparing(String::toString));
                    result.add(hashtagsList);
                }
            }
        }

        return result.iterator();
    }

    public static Iterator<Tuple2<List<String>, String>> extractHashtagTripletsAndUsers(String line){
        List<Tuple2<List<String>, String>> result = new ArrayList();
        List<String> hashtagsList = new ArrayList();
    
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }
    
        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                if(hashtags.length() == 3){ 
                    String user = retrieveUser(json);
                    if(user != null){
                        for(int i = 0; i < hashtags.length(); i++){
                            hashtagsList.add(hashtags.getJSONObject(i).getString("text"));
                        }
                        hashtagsList.sort(Comparator.comparing(String::toString));
                        Tuple2<List<String>, String> tuple = new Tuple2<List<String>, String>(hashtagsList, user);
                        result.add(tuple);
                    }
                }
            }
        }
    
        return result.iterator();
    }

    public static void runJob(JavaSparkContext context, JavaRDD<String> data, int k){

        //TopKHashtagTriplets
        List<Tuple2<List<String>, Integer>> test = data
            .flatMap(line -> extractHashtagTriplets(line))
            .mapToPair(hashtagTriplet -> new Tuple2<List<String>, Integer>(hashtagTriplet, 1))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorListString());

        JavaPairRDD<List<String>, Integer> test2 = context.parallelizePairs(test);

        //HashtagTriplets - User
        JavaPairRDD <List<String>, String> test3 = data
            .flatMapToPair( line -> extractHashtagTripletsAndUsers(line));

        //TopKHashtagsTriplets - User
        JavaPairRDD<List<String>, Tuple2<Integer, String>> test4 = test2.join(test3);

        //((TopKHashtagsTriplets, User), 1) - reduce - topk
        JavaPairRDD<List<String>, Tuple2<String, Integer>> test5 = test4
            .mapToPair((Tuple2<List<String>, Tuple2<Integer, String>> tuple) -> new Tuple2<>(new Tuple2<>(tuple._1, tuple._2._2), 1))
            .reduceByKey((a, b) -> a + b)
            .mapToPair((Tuple2<Tuple2<List<String>, String>, Integer> tuple) -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)))
            .reduceByKey((a,b) -> {
                if (a._2 > b._2){
                  return a;
                }
                else{
                  return b;
                }
              });
              

        System.out.println(test5.take(k));

    }
}
