package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class NbTweetLang extends SparkJob{

    public static Iterator<String> extractLangFromLine(String line){
        List<String> result = new ArrayList();
        
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String lang = null;
            try{
                lang = json.getString("lang");
            }catch(Exception e) { }

            if(lang != null){
                result.add(lang);
            }
        }

        return result.iterator();
    }

    public static void runJob(JavaSparkContext context, JavaRDD<String> data){
        JavaPairRDD<String, Integer> test = data
            .flatMap(line -> extractLangFromLine(line))
            .mapToPair(user -> new Tuple2<String, Integer>(user, 1))
            .reduceByKey((a, b) -> a + b);
    
        System.out.println(test.take(100));
    }
}

