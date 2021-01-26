package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import org.apache.hadoop.hbase.HBaseConfiguration;


import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import java.util.*;

public class NbTweetUser extends SparkJob{

    public static Iterator<Tuple2<String, Long>> extractUserFromLine(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            if(user != null){
                result.add(new Tuple2<String, Long>(user, 1L));
            }
        }

        return result.iterator();
    }

    public static void runJob() 
        throws MasterNotRunningException,IOException{

        JavaPairRDD<String, Long> rdd = GlobalManager.data
            .flatMapToPair(line -> extractUserFromLine(line))
            .reduceByKey((a, b) -> a + b);

        JavaRDD<Input<String, Long>> rddInput = rdd
            .map(elt -> new Input<String, Long>(elt._1,elt._2));

        GlobalManager.initTable("al-jda-user-tweet","total");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-user-tweet"))) {
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

