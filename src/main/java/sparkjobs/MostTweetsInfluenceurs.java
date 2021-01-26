package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.*;

import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

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

    public static void runJob(int k) throws MasterNotRunningException,IOException {

        //TopKHashtagTriplets
        List<Tuple2<List<String>, Long>> rdd = GlobalManager.data
            .flatMap(line -> extractHashtagTriplets(line))
            .mapToPair(hashtagTriplet -> new Tuple2<List<String>, Long>(hashtagTriplet, 1L))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorListString());

        JavaPairRDD<List<String>, Long> rdd2 = GlobalManager.context.parallelizePairs(rdd);

        //HashtagTriplets - User
        JavaPairRDD <List<String>, String> rdd3 = GlobalManager.data
            .flatMapToPair( line -> extractHashtagTripletsAndUsers(line));

        //TopKHashtagsTriplets - User
        JavaPairRDD<List<String>, Tuple2<Long, String>> rdd4 = rdd2.join(rdd3);

        //((TopKHashtagsTriplets, User), 1) - reduce - topk
        JavaPairRDD<List<String>, Tuple2<String, Long>> rdd5 = rdd4
            .mapToPair((Tuple2<List<String>, Tuple2<Long, String>> tuple) -> new Tuple2<>(new Tuple2<>(tuple._1, tuple._2._2), 1L))
            .reduceByKey((a, b) -> a + b)
            .mapToPair((Tuple2<Tuple2<List<String>, String>, Long> tuple) -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)))
            .reduceByKey((a,b) -> a._2 > b._2 ? a : b);

        JavaRDD<Input<String, Tuple2<String, Long>>> rddInput = rdd5
            .map(elt -> {
                String key = "";
                for(String s : elt._1)
                    key += "\n"+s;
                return new Input<String, Tuple2<String, Long>>(key, elt._2);
            });

        GlobalManager.initTable("al-jda-most-tweet-influencer","influencer");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-most-tweet-influencer"))) {
                    while (iterator.hasNext()) {
                        Input<String, Tuple2<String, Long>> input = iterator.next();
                        Put put1 = new Put(Bytes.toBytes(input.getKey()));
                        put1.addColumn(Bytes.toBytes("influencer"), Bytes.toBytes("name"), Bytes.toBytes(input.getMyValue()._1));
                        mutator.mutate(put1);
                        Put put2 = new Put(Bytes.toBytes(input.getKey()));
                        put2.addColumn(Bytes.toBytes("influencer"), Bytes.toBytes("name"), Bytes.toBytes(input.getMyValue()._2));
                        mutator.mutate(put2);
                    }
                }
            }
        );

    }
}
