package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.*;

public class Project {

    private static JavaSparkContext context;
    private static final int currentNbTweetFiles = 21;

    private static final String OUTPUT_URL = "/user/alegendre001/output/";
    private static String[] RESSOURCES_URLS = new String[21];

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

    public static void main(String[] args) throws IOException{
	    SparkConf sparkConf = new SparkConf().setAppName("Projet PLE 2020");
	    context = new JavaSparkContext(sparkConf);
        fillRessources();
        String allRessources = concateAllRessources();

        //JavaRDD<String> data = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
        // JavaRDD<String> allData = context.textFile(allRessources);
         JavaRDD<String> data = context.textFile(RESSOURCES_URLS[0]);

        // Instantiating configuration class
        Configuration hbaseConf = HBaseConfiguration.create();

        // Instantiating HBaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        // Verifying the existance of the table
        boolean bool = admin.tableExists("al-jda-database");
        if(!bool){
            HTableDescriptor tableDescriptor = new
            HTableDescriptor(TableName.valueOf("al-jda-database"));
            tableDescriptor.addFamily(new HColumnDescriptor("updates"));
            admin.createTable(tableDescriptor);
        }

        // TopKHashtag.runJob(context, hbaseConf, admin, data, 10);
        // HashtagListForUser.runJob(context, data);
        // HashtagTripletUser.runJob(context, data);
        //NbTweetLang.runJob(context, hbaseConf, admin, data);
        // NbTweetUser.runJob(context, data);
        // OccurenceHashtag.runJob(context, data);
        // TopKHashtagTriplet.runJob(context, data, 10);
        // UsedHashtagUser.runJob(context, data);
        //MostTweetsInfluenceurs.runJob(context, data, 10);
        FakeInfluenceur.runJob(context, data);

	    context.close();
	}
}

