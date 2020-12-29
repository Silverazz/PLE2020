package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class HashtagTripletUser extends SparkJob{

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

    public static void runJob(JavaSparkContext context, JavaRDD<String> data){
        JavaPairRDD <List<String>, Iterable<String>> test = data
            .flatMapToPair( line -> extractHashtagTripletsAndUsers(line))
            .distinct()
            .groupByKey();
    
        System.out.println(test.take(100));
    }
}

