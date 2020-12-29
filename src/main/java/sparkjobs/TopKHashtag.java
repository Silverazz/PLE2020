package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class TopKHashtag extends SparkJob{

    public static Iterator<String> extractHashtagsFromLine(String line){
        List<String> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                for(int i = 0; i < hashtags.length(); i++){
                    result.add(hashtags.getJSONObject(i).getString("text"));
                }
            }
        }

        return result.iterator();
    }

    public static void runJob(JavaSparkContext context, JavaRDD<String> data, int k){

        List<Tuple2<String, Integer>> test = data
            .flatMap(line -> extractHashtagsFromLine(line))
            .mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorString());

        JavaPairRDD<String, Integer> test2 = context.parallelizePairs(test);
        System.out.println(test2.take(k));
    }
}
