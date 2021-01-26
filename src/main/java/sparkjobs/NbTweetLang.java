package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import java.io.IOException;

import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import java.util.*;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.Writable;

public class NbTweetLang extends SparkJob{

    public static Iterator<Tuple2<String, Long>> extractLangFromLine(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        
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
                result.add(new Tuple2<String, Long>(lang, 1L));
            }
        }

        return result.iterator();
    }

    public static void runJob() 
        throws MasterNotRunningException,IOException{

        JavaPairRDD<String, Long> rdd = GlobalManager.data
            .flatMapToPair(line -> extractLangFromLine(line))
            .reduceByKey((a, b) -> a + b);
    
        System.out.println(rdd.take(100));

        JavaRDD<Input<String, Long>> rddInput = rdd.map(elt -> new Input<String, Long>(elt._1,elt._2));

        GlobalManager.initTable("al-jda-lang","total");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(GlobalManager.hbaseConf);
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-lang"))) {
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

