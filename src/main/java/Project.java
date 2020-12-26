package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import org.json.*;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.lang.Math;
import java.io.Serializable;

public class Project {

    private static JavaSparkContext context;
    private static final int currentNbTweetFiles = 21;

    private static final String OUTPUT_URL = "/user/alegendre001/output/";
    private static String[] RESSOURCES_URLS = new String[21];

    public static void fillRessources(){
        for(int i = 0; i < currentNbTweetFiles; i++){
            String tweetDay = String.valueOf(i+1);
            if(i < 9){
                tweetDay = "0" + tweetDay; 
            }
            RESSOURCES_URLS[i] = "/raw_data/tweet_" + tweetDay + "_03_2020.nljson";
        }
    }

    public static void topK(JavaRDD<String> data){
        

        //JavaRDD<String> output = context.parallelize(result);

       // ArrayList<String> top = output.top(10);
    }

    public static Iterator<String> extractHashtagsFromLine(String line){
        List<String> result = new ArrayList();

        JSONObject json = new JSONObject(line);

        JSONObject entities = null;
        try {
            entities = json.getJSONObject("entities");
        }catch(Exception e) {
            
        }

        if(entities == null){
            return result.iterator();
        }

        JSONArray hashtags = null;
        try {
            hashtags = entities.getJSONArray("hashtags");
        }catch(Exception e) {
            
        }

        if(hashtags == null){
            return result.iterator();
        }

        for(int i = 0; i < hashtags.length(); i++){
            result.add(hashtags.getJSONObject(i).getString("text"));
        }

        return result.iterator();
    }

    public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setAppName("Projet PLE 2020");
	    context = new JavaSparkContext(conf);
        fillRessources();

        //Day 01 for start
        JavaRDD<String> lines = context.textFile(RESSOURCES_URLS[2]);
/*
        JavaPairRDD<String, Integer> = lines
            .flatMap(line -> Arrays.asList(line.split(",")).iterator())
            .mapToPair(hashtag -> new Tuple2<>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(10);*/

        //JavaRDD<String> test = lines.flatMap(line -> Arrays.asList(line.split(",")).iterator());
        //JavaRDD<String> tryt = test.filter(line -> line.contains("hashtags"));

        //System.out.println(lines.take(3));

        JavaPairRDD<String, Integer> test = context.parallelizePairs(lines
            .flatMap(line -> extractHashtagsFromLine(line))
            .mapToPair(hashtag -> new Tuple2<>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(10));

        System.out.println(test.take(10));


	    context.close();
	}
}