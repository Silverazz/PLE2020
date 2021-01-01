package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.*;

public class FakeInfluenceur extends SparkJob{

    private static final int nbFollowersInfluenceur = 10000;

    public static Iterator<Tuple2<String, Integer>> extractFakeInfluenceurAndFollowers(String line){
        List<Tuple2<String, Integer>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            int nbFollowers = retrieveNbFollowers(json);
            int nbReweets = retrieveNbRetweets(json);
            if(user != null && nbFollowers >= nbFollowersInfluenceur && nbReweets == 0){
                Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(user, nbFollowers);
                result.add(tuple);
            }
        }

        return result.iterator();
    }

    public static Iterator<Tuple2<String, Integer>> extractRealInfluenceurAndFollowers(String line){
        List<Tuple2<String, Integer>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            int nbFollowers = retrieveNbFollowers(json);
            int nbReweets = retrieveNbRetweets(json);
            if(user != null && nbFollowers >= nbFollowersInfluenceur && nbReweets > 0){
                Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(user, nbFollowers);
                result.add(tuple);
            }
        }

        return result.iterator();
    }
    

    public static void runJob(JavaSparkContext context, JavaRDD<String> data){

        JavaPairRDD<String, Integer> test = data
            .flatMapToPair(line -> extractFakeInfluenceurAndFollowers(line))
            .distinct();

        JavaPairRDD<String, Integer> test2 = data
            .flatMapToPair(line -> extractRealInfluenceurAndFollowers(line))
            .distinct();

        JavaPairRDD<String, Integer> test3 = test.subtract(test2);

        System.out.println(test3.take(20));

    }
}
