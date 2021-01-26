package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

import java.io.IOException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

public class UsedHashtagUser extends SparkJob{

    public static Iterator<String> extractUserUsedHashtagFromLine(String line){
        List<String> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null && hashtags.length() > 0){
                String user = retrieveUser(json);
                if(user != null){
                    result.add(user);
                }
            }
        }

        return result.iterator();
    }

    public static void runJob() throws MasterNotRunningException,IOException {
        JavaRDD<String> test = GlobalManager.data
            .flatMap(line -> extractUserUsedHashtagFromLine(line))
            .distinct();

        System.out.println(test.take(10));
    }
}

