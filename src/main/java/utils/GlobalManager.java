package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HColumnDescriptor;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import org.apache.spark.SparkConf;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class GlobalManager {

    public static JavaSparkContext context;
    public static Configuration hbaseConf;
    public static JavaRDD<String> data;
    
    private static final int currentNbTweetFiles = 21;
    private static String[] RESSOURCES_URLS = new String[21];

    public static void initEnv(int day){
        SparkConf sparkConf = new SparkConf().setAppName("Projet PLE 2020");
        context = new JavaSparkContext(sparkConf);
        hbaseConf = HBaseConfiguration.create();
        setData(day);
    }

    public static void close() {
	    context.close();
    }

    public static void setData(int value){
        if(value == -2)
            data = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
        else{
            fillRessources();
            
            if(value == -1){
                String allRessources = concateAllRessources();
                data = context.textFile(allRessources);
            }

            if(value > 0)
                data = context.textFile(RESSOURCES_URLS[value]);
        }
    }

    public static void fillRessources(){
        for(int i = 0; i < currentNbTweetFiles; i++){
            String tweetDay = String.valueOf(i+1);
            if(i < 9){
                tweetDay = "0" + tweetDay; 
            }
            RESSOURCES_URLS[i] = "/raw_data/tweet_" + tweetDay + "_03_2020.nljson";
        }
    }

    public static String concateAllRessources(){
        String allRessources = RESSOURCES_URLS[0];
        for(int i = 1; i < currentNbTweetFiles; i++){
            allRessources = allRessources.concat("," + RESSOURCES_URLS[i]);
        }
        return allRessources;
    }

    public static void initTable(String table, String columnFamily) 
        throws MasterNotRunningException,IOException{

        // Instantiating HBaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        // Verifying the existance of the table
        boolean bool = admin.tableExists(table);
        if(!bool){
            HTableDescriptor tableDescriptor = new
            HTableDescriptor(TableName.valueOf(table));
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
        }
        else{
            // Create the column only if it does not already exists
            HTable hTable = new HTable(hbaseConf, table);
            if(!hTable.getTableDescriptor().hasFamily(Bytes.toBytes(columnFamily))){
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                admin.addColumn(table, columnDescriptor);
            }
        }
    }

    // public static void saveRDD(String tableName, String columnFamily, 
    //     String column, JavaRDD<Input> rddInput) throws IOException {
    //         Configuration config = new Configuration();
    //         config.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    //         Job jobConfig = Job.getInstance(config);
    //         jobConfig.setOutputFormatClass(TableOutputFormat.class);
    //         rddInput.mapToPair(input -> {
    //             Put put = new Put(Bytes.toBytes(input.getKey()));
    //             put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(input.getMyValue()));
    //             return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(put.getRow()), put);
    //         }).saveAsNewAPIHadoopDataset(jobConfig.getConfiguration());
    //     }
}
