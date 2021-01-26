package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.*;
import scala.Tuple2;

import java.util.*;

public class TopKHashtagTriplet extends SparkJob{

    public static Iterator<Tuple2<String, Long>> extractHashtagTriplets(String line){
        List<Tuple2<String, Long>> result = new ArrayList();
        List<String> hashtagsList = new ArrayList();  

        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                if(hashtags.length() == 3){ 
                    for(int i = 0; i < hashtags.length(); i++){
                        hashtagsList.add(hashtags.getJSONObject(i).getString("text"));
                    }
                    hashtagsList.sort(Comparator.comparing(String::toString));

                    String str = "";
                    for(int i = 0; i < hashtagsList.size(); i++){
                        str += "\n"+hashtagsList.get(i);
                    }
                    result.add(new Tuple2<String, Long>(str, 1L));
                }
            }
        }

        return result.iterator();
    }

    public static void runJob(int k){
        List<Tuple2<String, Long>> test = GlobalManager.data
            .flatMapToPair(line -> extractHashtagTriplets(line))
            .reduceByKey((a, b) -> a + b)
            .top(k, new TupleComparatorString());

        

        JavaPairRDD<String, Long> test2 = GlobalManager.context.parallelizePairs(test);



        System.out.println(test2.take(k));

        
    }
}

// package bigdata;

// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.json.*;
// import scala.Tuple2;

// import java.util.*;

// public class TopKHashtagTriplet extends SparkJob{

//     public static Iterator<Tuple2<List<String>, Long>> extractHashtagTriplets(String line){
//         List<Tuple2<List<String>, Long>> result = new ArrayList();
//         List<String> hashtagsList = new ArrayList();  

//         JSONObject json = null;
//         try {
//             json = new JSONObject(line);
//         }catch(Exception e){ }

//         if(json != null){
//             JSONArray hashtags = retrieveHashtags(json);
//             if(hashtags != null){
//                 if(hashtags.length() == 3){ 
//                     for(int i = 0; i < hashtags.length(); i++){
//                         hashtagsList.add(hashtags.getJSONObject(i).getString("text"));
//                     }
//                     hashtagsList.sort(Comparator.comparing(String::toString));
//                     result.add(new Tuple2<List<String>, Long>(hashtagsList, 1));
//                 }
//             }
//         }

//         return result.iterator();
//     }

//     public static void runJob(int k){
//         List<Tuple2<List<String>, Long>> rdd = GlobalManager.data
//             .flatMapToPair(line -> extractHashtagTriplets(line))
//             .reduceByKey((a, b) -> a + b)
//             .top(k, new TupleComparatorListString());

//         JavaRDD<Input<Long, Tuple2<String, Long>>> rddInput = GlobalManager.context
//             .parallelize(rdd)
//             .zipWithIndex()
//             .map(elt -> {
//                 String str = "";
//                 for(int i = 0; i < elt._1.size(); i++)
//                     str += "#"+elt._1[i];
//                 return new Input<Long, Tuple2<String, Long>>(elt._2, elt._1);
//             });

//         GlobalManager.initTable("al-jda-top-hashtag-triplet","hashtag");

//         rddInput.foreachPartition(iterator -> {
//             try (Connection connection = ConnectionFactory.createConnection(GlobalManager.hbaseConf);
//                 BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf("al-jda-top-hashtag-triplet"))) {
//                     while (iterator.hasNext()) {
//                         Input<Long, Tuple2<String, Long>> input = iterator.next();
//                         Put put1 = new Put(Bytes.toBytes(input.getKey()));
//                         put1.addColumn(Bytes.toBytes("hashtag"), Bytes.toBytes("name"), Bytes.toBytes(input.getMyValue()._1));
//                         mutator.mutate(put1);
//                         Put put2 = new Put(Bytes.toBytes(input.getKey()));
//                         put2.addColumn(Bytes.toBytes("hashtag"), Bytes.toBytes("number"), Bytes.toBytes(input.getMyValue()._2));
//                         mutator.mutate(put2);
//                     }
//                 }
//             }
//         );
//     }
// }

