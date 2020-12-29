package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class TopKHashtagTriplet extends SparkJob{

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

    public static void runJob(JavaSparkContext context, JavaRDD<String> data, int k){
        List<Tuple2<List<String>, Integer>> test = data
            .flatMap(line -> extractHashtagTriplets(line))
            .mapToPair(hashtagTriplet -> new Tuple2<List<String>, Integer>(hashtagTriplet, 1))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorListString());

        JavaPairRDD<List<String>, Integer> test2 = context.parallelizePairs(test);
        System.out.println(test2.take(k));
    }
}
