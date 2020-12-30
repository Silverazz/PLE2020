package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HColumnDescriptor;
import java.io.IOException;

import java.util.*;

public class TopKHashtag extends SparkJob{

    public static Iterator<Tuple2<String, Integer>> extractHashtagsFromLine(String line){
        List<Tuple2<String, Integer>> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                for(int i = 0; i < hashtags.length(); i++){
                    String hashtag = hashtags.getJSONObject(i).getString("text");
                    Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(hashtag,1);
                    result.add(tuple);
                }
            }
        }

        return result.iterator();
    }

    public static void runJob(JavaSparkContext context, Configuration hbaseConf, 
        HBaseAdmin admin, JavaRDD<String> data, int k) throws MasterNotRunningException,IOException{

        List<Tuple2<String, Integer>> test = data
            .flatMapToPair(line -> extractHashtagsFromLine(line))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorString());

        JavaRDD<Tuple2<String, Integer>> test2 = context.parallelize(test);
        System.out.println(test2.take(k));

        HTable hTable = new HTable(hbaseConf, "al-jda-database");

        if(!hTable.getTableDescriptor().hasFamily(Bytes.toBytes("topKHashtag"))){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("topKHashtag");
            admin.addColumn("al-jda-database", columnDescriptor);
        }

        // test2.foreachPartition(iterator -> {
        //     try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        //          //option 1.1 is to use Table table = connection.getTable(TableName.valueOf(tableName));
        //         BufferedMutator mutator = connection.getBufferedMutator(hTable.getName())) {
        //             while (iterator.hasNext()) {
        //                 Tuple2<String, Integer> line = iterator.next();
        //                 Put put = new Put(Bytes.toBytes(line._1));
        //                 put.addColumn(Bytes.toBytes("topKHashtag"), Bytes.toBytes("count"), Bytes.toBytes(line._2));
        //                 mutator.mutate(put);
        //                 //table.put(put);
        //             }
        //         }
        //     }
        // );

        // Configuration config = new Configuration();
        // config.set(TableOutputFormat.OUTPUT_TABLE, "al-jda-databases");
        // Job jobConfig = Job.getInstance(config);
        // jobConfig.setOutputFormatClass(TableOutputFormat.class);
        // test2.mapToPair(record -> {
        //     Put put = new Put(Bytes.toBytes(record.getKey()));
        //     put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("v"), Bytes.toBytes(record.getMyValue()));
        //     return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(put.getRow()), put);
        // }).saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
    }
}
