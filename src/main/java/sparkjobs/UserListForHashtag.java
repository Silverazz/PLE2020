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

public class UserListForHashtag extends SparkJob{

    public static Iterator<Tuple2<String, String>> extractTupleHashtagUserFromLine(String line){
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
                    Tuple2<String, String> tuple = new Tuple2<String, String>(hashtag, user);
                    result.add(tuple);
                }
            }
        }

        return result.iterator();
    }

    public static void runJob() throws MasterNotRunningException,IOException {
        JavaPairRDD<String, Iterable<String>> rdd = GlobalManager.data
            .flatMapToPair(line -> extractTupleHashtagUserFromLine(line))
            .distinct()
            .groupByKey();

        JavaRDD<Input<String, String>> rddInput = rdd
            .map(elt -> {
                String str = "";
                for(String s : elt._2){
                    str += "\n"+s;
                }
                return new Input<String, String>(elt._1,str);
            });

        GlobalManager.initTable("al-jda-hashtag-user-list","user");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-hashtag-user-list"))) {
                    while (iterator.hasNext()) {
                        Input<String, String> input = iterator.next();
                        Put put = new Put(Bytes.toBytes(input.getKey()));
                        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("list"), Bytes.toBytes(input.getMyValue()));
                        mutator.mutate(put);
                    }
                }
            }
        );
    }
}

