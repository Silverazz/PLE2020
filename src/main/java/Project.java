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

    public static String concateAllRessources(){
        String allRessources = RESSOURCES_URLS[0];
        for(int i = 1; i < currentNbTweetFiles; i++){
            allRessources = allRessources.concat("," + RESSOURCES_URLS[i]);
        }
        return allRessources;
    }

    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
            return Integer.compare(tuple1._2, tuple2._2);
        }
    }

    /*################ RETRIEVE INFO JSON ################*/

    public static JSONArray retrieveHashtags(JSONObject json){
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

    /*################ EXTRACT FROM DATA LINE ################*/

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

    public static Iterator<String> extractUserUsedHashtagFromLine(String line){
        List<String> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null && hashtags.length() > 0){
                String user = retrieveUser(json);
                if(user != null){
                    result.add(user);
                }
            }
        }

        return result.iterator();
    }

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

    public static Iterator<String> extractUserFromLine(String line){
        List<String> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            if(user != null){
                result.add(user);
            }
        }

        return result.iterator();
    }

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

    /*################ RDDs functions ################*/

    /*Hashtag a)*/
    public static void topkHashtags(JavaRDD<String> data, int k){

        List<Tuple2<String, Integer>> test = data
            .flatMap(line -> extractHashtagsFromLine(line))
            .mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparator());

        JavaPairRDD<String, Integer> test2 = context.parallelizePairs(test);
        System.out.println(test2.take(k));
    }

    /*Hashtag c)*/
    public static void occurencesHashtags(JavaRDD<String> data){

        JavaPairRDD<String, Integer> test = data
            .flatMap(line -> extractHashtagsFromLine(line))
            .mapToPair(hashtag -> new Tuple2<String, Integer>(hashtag, 1))
            .reduceByKey((a, b) -> a + b);

        System.out.println(test.take(10));
    }

    /*Hashtag d)*/
    public static void usedHashtagsUsers(JavaRDD<String> data){

        JavaRDD<String> test = data
            .flatMap(line -> extractUserUsedHashtagFromLine(line))
            .distinct();

        System.out.println(test.take(10));
    }

    /*User a)*/
    public static void hashtagListForUser(JavaRDD<String> data){
        JavaPairRDD <String, Iterable<String>> test = data
            .flatMapToPair( line -> extractTupleUserHashtagFromLine(line))
            .distinct()
            .groupByKey();

        System.out.println(test.take(100));
    }

    /*User b)*/
    public static void nbTweetsUser(JavaRDD<String> data){
        JavaPairRDD<String, Integer> test = data
            .flatMap(line -> extractUserFromLine(line))
            .mapToPair(user -> new Tuple2<String, Integer>(user, 1))
            .reduceByKey((a, b) -> a + b);

        System.out.println(test.take(100));
    }

    /*User c)*/
    public static void nbTweetsLang(JavaRDD<String> data){
        JavaPairRDD<String, Integer> test = data
            .flatMap(line -> extractLangFromLine(line))
            .mapToPair(user -> new Tuple2<String, Integer>(user, 1))
            .reduceByKey((a, b) -> a + b);
    
        System.out.println(test.take(100));
    }

    /*Influenceur a)*/
    public static void hashtagTripletsUsers(JavaRDD<String> data){
        JavaPairRDD <List<String>, Iterable<String>> test = data
            .flatMapToPair( line -> extractHashtagTripletsAndUsers(line))
            .distinct()
            .groupByKey();

        System.out.println(test.take(100));
    }

    public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("Projet PLE 2020");
	    context = new JavaSparkContext(conf);
        fillRessources();
        String allRessources = concateAllRessources();

        //Day 01 to start
        JavaRDD<String> data = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");

        //Topk a)
        //int k = 10;
        //topkHashtags(allData, k);

        //Topk b)
        //JavaRDD<String> allData = context.textFile(allRessources);
        //int k =10;
        //topKHashtags(allData, k);

        //nb apparitions c)
        //occurencesHashtags(data);

        //hashtags user d)
        //usedHashtagsUsers(data);

        //user a)
        //hashtagListForUser(data);

        //user b)
        //nbTweetsUser(data);

        //user c)
        //nbTweetsLang(data);

        //influenceurs a)
        hashtagTripletsUsers(data);

	    context.close();
	}
}
