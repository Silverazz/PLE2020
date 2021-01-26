package bigdata;

import java.io.IOException;

public class Project {

    public static void main(String[] args) throws IOException{

        /* -2 : first 10000.
         * -1 : all days.
         * [1, ..., 21] : specific day. 
        */
        GlobalManager.initEnv(-2);

        // NbTweetUser.runJob();
        TopKHashtag.runJob(10000);

        // Checked

        // NbTweetLang.runJob();
        // OccurenceHashtag.runJob();
        
        // FakeInfluenceur.runJob();

        // To check

        // TopKHashtagTriplet.runJob(10);

        // JavaRDD<String> data = context.textFile("/raw_data/tweet_01_03_2020_first10000.nljson");
        //JavaRDD<String> allData = context.textFile(allRessources);
        //JavaRDD<String> data = context.textFile(RESSOURCES_URLS[0]);

        // Instantiating configuration class
        //Configuration hbaseConf = HBaseConfiguration.create();

        // Instantiating HBaseAdmin class
        ////HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        // Verifying the existance of the table
        /*boolean bool = admin.tableExists("al-jda-database");
        if(!bool){
            HTableDescriptor tableDescriptor = new
            HTableDescriptor(TableName.valueOf("al-jda-database"));
            tableDescriptor.addFamily(new HColumnDescriptor("updates"));
            admin.createTable(tableDescriptor);
        }*/

        // HashtagListForUser.runJob(context, data);
        // HashtagTripletUser.runJob(context, data);
        // UsedHashtagUser.runJob(context, data);
        // MostTweetsInfluenceurs.runJob(context, data, 10);
        //MostTweetsInfluenceurs.runJob(context, data, 10);
        //FakeInfluenceur.runJob(context, data);
        //HashtagMostFollowers.runJob(context, allData);
        // RetagTweets.runJob(context, data);

        GlobalManager.close();
	}
}

