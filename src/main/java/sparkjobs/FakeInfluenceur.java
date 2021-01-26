package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;

public class FakeInfluenceur extends SparkJob{

    private static final int nbFollowersInfluenceur = 10000;

    public static Iterator<Tuple2<String, Long>> extractFakeInfluencerAndFollowers(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            long nbFollowers = retrieveNbFollowers(json);
            int nbReweets = retrieveNbRetweets(json);
            if(user != null && nbFollowers >= nbFollowersInfluenceur && nbReweets == 0){
                Tuple2<String, Long> tuple = new Tuple2<String, Long>(user, nbFollowers);
                result.add(tuple);
            }
        }

        return result.iterator();
    }

    public static Iterator<Tuple2<String, Long>> extractRealInfluencerAndFollowers(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        JSONObject json = null;

        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String user = retrieveUser(json);
            long nbFollowers = retrieveNbFollowers(json);
            int nbReweets = retrieveNbRetweets(json);
            if(user != null && nbFollowers >= nbFollowersInfluenceur && nbReweets > 0){
                Tuple2<String, Long> tuple = new Tuple2<String, Long>(user, nbFollowers);
                result.add(tuple);
            }
        }

        return result.iterator();
    }
    

    public static void runJob() 
        throws MasterNotRunningException,IOException{

        JavaPairRDD<String, Long> rddFakeInluencer = GlobalManager.data
            .flatMapToPair(line -> extractFakeInfluencerAndFollowers(line))
            .distinct();

        JavaPairRDD<String, Long> rddRealInlfuencer = GlobalManager.data
            .flatMapToPair(line -> extractRealInfluencerAndFollowers(line))
            .distinct();

        JavaPairRDD<String, Long> rdd = rddFakeInluencer.subtract(rddRealInlfuencer);

        System.out.println(rdd.take(20));

        JavaRDD<Input<String, Long>> rddInput = rdd
            .map(elt -> new Input<String, Long>(elt._1,elt._2));

        GlobalManager.initTable("al-jda-fake-influencer","total");

        rddInput.foreachPartition(iterator -> {
            try (Connection connection = ConnectionFactory.createConnection(GlobalManager.hbaseConf);
                BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-fake-influencer"))) {
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
