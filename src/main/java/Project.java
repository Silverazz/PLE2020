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

        // HashtagListForUser.runJob(context, data);
        // HashtagTripletUser.runJob(context, data);
        // UsedHashtagUser.runJob(context, data);
        // MostTweetsInfluenceurs.runJob(context, data, 10);

        GlobalManager.close();
	}
}

