package bigdata;

import java.io.IOException;

public class Project {

    public static void main(String[] args) throws IOException{

        /* -2 : first 10000.
         * -1 : all days.
         * [1, ..., 21] : specific day. 
        */
        GlobalManager.initEnv(-1);

        // TopKHashtag.runJob(10000);
        NbTweetUser.runJob();

        GlobalManager.close();
	}
}

