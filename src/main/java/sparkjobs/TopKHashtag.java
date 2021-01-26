package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.json.*;
import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.*;

public class TopKHashtag extends SparkJob{

    public static Iterator<Tuple2<String, Long>> extractHashtagsFromLine(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                for(int i = 0; i < hashtags.length(); i++){
                    String hashtag = hashtags.getJSONObject(i).getString("text");
                    Tuple2<String, Long> tuple = new Tuple2<String, Long>(hashtag,1L);
                    result.add(tuple);
                }
            }
        }

        return result.iterator();
    }

    public static void runJob(int k) throws MasterNotRunningException,IOException{

        List<Tuple2<String, Long>> rdd = GlobalManager.data
            .flatMapToPair(line -> extractHashtagsFromLine(line))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorString());

        JavaRDD<Input<Long, Tuple2<String, Long>>> rddInput = GlobalManager.context
            .parallelize(rdd)
            .zipWithIndex()
            .map(elt -> new Input<Long, Tuple2<String, Long>>(elt._2, elt._1));

        GlobalManager.initTable("al-jda-top-hashtag","hashtag");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-hashtag"))) {
                    while (iterator.hasNext()) {
                        Input<Long, Tuple2<String, Long>> input = iterator.next();
                        Put put1 = new Put(Bytes.toBytes(input.getKey()));
                        put1.addColumn(Bytes.toBytes("hashtag"), Bytes.toBytes("name"), Bytes.toBytes(input.getMyValue()._1));
                        mutator.mutate(put1);
                        Put put2 = new Put(Bytes.toBytes(input.getKey()));
                        put2.addColumn(Bytes.toBytes("hashtag"), Bytes.toBytes("number"), Bytes.toBytes(input.getMyValue()._2));
                        mutator.mutate(put2);
                    }
                }
            }
        );
        
    }
}
