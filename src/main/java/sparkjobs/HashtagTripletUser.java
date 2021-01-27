package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

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

    public static void runJob() throws MasterNotRunningException,IOException{
        JavaPairRDD <List<String>, Iterable<String>> rdd = GlobalManager.data
            .flatMapToPair( line -> extractHashtagTripletsAndUsers(line))
            .distinct()
            .groupByKey();
    
        JavaRDD<Input<String, String>> rddInput = rdd
            .map(elt -> {
                String key = "";
                String value = "";
                for(String s : elt._1)
                    key += "\n"+s;
                for(String s : elt._2)
                    value += "\n"+s;
                return new Input<String, String>(key, value);
            });

        GlobalManager.initTable("al-jda-hashtag-triplet-user","users");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-hashtag-triplet-user"))) {
                    while (iterator.hasNext()) {
                        Input<String, String> input = iterator.next();
                        Put put = new Put(Bytes.toBytes(input.getKey()));
                        put.addColumn(Bytes.toBytes("users"), Bytes.toBytes("list"), Bytes.toBytes(input.getMyValue()));
                        mutator.mutate(put);
                    }
                }
            }
        );
    }
}

