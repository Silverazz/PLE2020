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

import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import java.util.*;

public class NbTweetLang extends SparkJob{

    public static Iterator<Tuple2<String, Integer>> extractLangFromLine(String line){
        List<Tuple2<String, Integer>> result = new ArrayList();
        
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
                result.add(new Tuple2<String, Integer>(lang, 1));
            }
        }

        return result.iterator();
    }

    public static class MyRecord implements Serializable {

        private String key;
        private Integer myValue;

        public MyRecord(String key, Integer myValue) {
            this.key = key;
            this.myValue = myValue;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Integer getMyValue() {
            return myValue;
        }

        public void setMyValue(Integer myValue) {
            this.myValue = myValue;
        }
    }

    public static void runJob(JavaSparkContext context, Configuration hbaseConf, 
        HBaseAdmin admin, JavaRDD<String> data) throws MasterNotRunningException,IOException{

        JavaPairRDD<String, Integer> test = data
            .flatMapToPair(line -> extractLangFromLine(line))
            .reduceByKey((a, b) -> a + b);
    
        System.out.println(test.take(100));

        HTable hTable = new HTable(hbaseConf, "al-jda-database");

        if(!hTable.getTableDescriptor().hasFamily(Bytes.toBytes("nbTweetLang"))){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("nbTweetLang");
            admin.addColumn("al-jda-database", columnDescriptor);
        }

        JavaRDD<MyRecord> rdd = test.map(elt -> new MyRecord(elt._1,elt._2));

        rdd.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-database"))) {
                    while (iterator.hasNext()) {
                        MyRecord record = iterator.next();
                        Put put = new Put(Bytes.toBytes(record.getKey()));
                        put.addColumn(Bytes.toBytes("nbTweetLang"),Bytes.toBytes("total"), Bytes.toBytes(record.getMyValue()));
                        mutator.mutate(put);
                    }
                }
            }
        );
    }
}

