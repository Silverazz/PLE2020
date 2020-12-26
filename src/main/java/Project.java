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
import java.beans.Transient;
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

    
    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
        }
    }

    public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setAppName("Projet PLE 2020");
	    context = new JavaSparkContext(conf);
        fillRessources();

        //Day 01 for start
        JavaRDD<String> lines = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
/*
        JavaPairRDD<String, Integer> = lines
            .flatMap(line -> Arrays.asList(line.split(",")).iterator())
            .mapToPair(hashtag -> new Tuple2<>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(10);*/

        //JavaRDD<String> test = lines.flatMap(line -> Arrays.asList(line.split(",")).iterator());
        //JavaRDD<String> tryt = test.filter(line -> line.contains("hashtags"));

        //System.out.println(lines.take(3));

        
        //TupleComparator tupleComparator = new TupleComparator();

        List<Tuple2<String, Integer>> test = lines
            .flatMap(line -> extractHashtagsFromLine(line))
            .mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(10, new TupleComparator());

        JavaPairRDD<String, Integer> test2 = context.parallelizePairs(test);

        System.out.println(test2.take(10));

	    context.close();
	}
}
