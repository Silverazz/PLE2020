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

public class HashtagMostFollowers extends SparkJob{

    public static Iterator<Tuple2<Tuple2<String, String>, Long>> extractHashtagUserAndNbFollowers(String line){
        List<Tuple2<Tuple2<String, String>, Long>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            if(user != null){
                Long nbFollowers = Long.valueOf(retrieveNbFollowers(json));
                if(nbFollowers > 0){
                    JSONArray hashtags = retrieveHashtags(json);
                    if(hashtags != null){
                        if(hashtags.length() > 0){ 
                            for(int i = 0; i < hashtags.length(); i++){
                                String hashtag = hashtags.getJSONObject(i).getString("text");
                                Tuple2<String, String> tupleHashtagUser = new Tuple2<String, String>(hashtag, user);
                                Tuple2<Tuple2<String, String>, Long> tupleHashtagUserNbFollowers = new Tuple2<Tuple2<String, String>, Long>(tupleHashtagUser, nbFollowers);
                                result.add(tupleHashtagUserNbFollowers);
                            }
                        }
                    }
                }
            }
        }
        return result.iterator();
    }
    
    public static void runJob(int k)throws MasterNotRunningException,IOException {

        List<Tuple2<String, Long>> rdd = GlobalManager.data
            .flatMapToPair(line -> extractHashtagUserAndNbFollowers(line))
            .distinct()
            .mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorLong());


        JavaRDD<Input<String, Long>> rddInput = GlobalManager.context
            .parallelize(rdd)
            .map(elt -> new Input<String, Long>(elt._1, elt._2));

        GlobalManager.initTable("al-jda-user-hashtag-most-followers","total");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-user-hashtag-most-followers"))) {
                    while (iterator.hasNext()) {
                        Input<String, Long> input = iterator.next();
                        Put put = new Put(Bytes.toBytes(input.getKey()));
                        put.addColumn(Bytes.toBytes("total"), Bytes.toBytes("value"), Bytes.toBytes(input.getMyValue()));
                        mutator.mutate(put);
                    }
                }
            }
        );

    }
}