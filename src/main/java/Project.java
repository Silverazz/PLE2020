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
<<<<<<< HEAD
        // NbTweetUser.runJob();
        FakeInfluenceur.runJob();
=======
        //NbTweetUser.runJob();
        //UserListForHashtag.runJob();
>>>>>>> 0b513b0344bd3d52821d7a8863db3c4e294021e7

        GlobalManager.close();
	}
}

