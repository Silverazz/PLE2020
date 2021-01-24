package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import org.json.*;
import scala.Tuple2;

import java.util.regex.*;
import java.util.*;
import java.util.stream.*;

public class RetagTweets extends SparkJob{

    public static Iterator<Tuple2<String, Integer>> extractHashtagsFromLine(String line){
        List<Tuple2<String, Integer>> result = new ArrayList();
        JSONObject json = null;
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                for(int i = 0; i < hashtags.length(); i++){
                    String hashtag = hashtags.getJSONObject(i).getString("text");
                    Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(hashtag,1);
                    result.add(tuple);
                }
            }
        }

        return result.iterator();
    }

    private static List<String> getWordsFromTextFromTweet(String text){
        String[] textSplit = text.split(" ");
        List<String> wordsList = new ArrayList<String>(Arrays.asList(textSplit));

        for(String word: wordsList){
            if(word.charAt(0) == '#' || word.length() == 1){
                wordsList.remove(word);
            }
        }

        List<String> wordsNoDuplicates = wordsList.stream()
            .distinct()
            .collect(Collectors.toList());
        
        return wordsNoDuplicates;
    }

    public static Iterator<Tuple2<String, String>> extractWordsFromText(String line){
        List<Tuple2<String, String>> result = new ArrayList();
        JSONObject json = null;
        
        try {
            json = new JSONObject(line);
        }catch(Exception e){ }

        if(json != null){
            String textFromTweet = null;
            try { 
                textFromTweet = json.getString("text");
            }catch(Exception e){ }

            if(textFromTweet != null){
                List<String> wordsFromText = getWordsFromTextFromTweet(textFromTweet);
                for(int i = 0; i < wordsFromText.size(); i++){
                    Tuple2<String, String> wordAndTweet = new Tuple2<>(wordsFromText.get(i), line);
                    result.add(wordAndTweet);
                }
            }
        }

        return result.iterator();
    }

    public static Tuple2<Long, String> replaceKeyByTweetIdAndModifyTweet(Tuple2<String, Tuple2<Integer, String>> tuple){
        JSONObject json = null;
        String hashtagToAdd = tuple._1;
        String tweetString = tuple._2._2;
        
        try {
            json = new JSONObject(tweetString);
        }catch(Exception e){ }

        if(json != null){
            //First assign new hashtag in hashtags list
            JSONArray hashtags = retrieveHashtags(json);
            if(hashtags != null){
                JSONObject hashtagJsonObject = new JSONObject();
                hashtagJsonObject.put("text", hashtagToAdd);
                hashtags.put(hashtagJsonObject);

                //Second modify text and put hashtag in text
                String textFromTweet = null;
                try { 
                    textFromTweet = json.getString("text");
                }catch(Exception e){ }

                if(textFromTweet != null){
                    String[] textSplit = textFromTweet.split(" ");
                    List<String> wordsList = new ArrayList<String>(Arrays.asList(textSplit));

                    for(int i = 0; i < wordsList.size(); i++){
                        if(wordsList.get(i) == hashtagToAdd){
                            wordsList.set(i, "#" + hashtagToAdd);
                        }
                    }

                    String newText = String.join(" ", wordsList);
                    json.put("text", newText);
                }
            }
        }

        Long tweetId = retrieveTweetId(json);
        return new Tuple2<>(tweetId, json.toString());
    }

    public static String mergeTweets(String tweetA, String tweetB){
        JSONObject jsonA = null;

        try {
            jsonA = new JSONObject(tweetA);
        }catch(Exception e){ }

        JSONObject jsonB = null;

        try {
            jsonB = new JSONObject(tweetB);
        }catch(Exception e){ }

        if(jsonA != null && jsonB != null){
            String textFromTweetA = null;
            
            try { 
                textFromTweetA = jsonA.getString("text");
            }catch(Exception e){ }

            String textFromTweetB = null;

            try { 
                textFromTweetB = jsonB.getString("text");
            }catch(Exception e){ }

            //merge texts
            if(textFromTweetA != null && textFromTweetB != null){
                String[] textSplitA = textFromTweetA.split(" ");
                List<String> wordsListA = new ArrayList<String>(Arrays.asList(textSplitA));

                String[] textSplitB = textFromTweetB.split(" ");
                List<String> wordsListB = new ArrayList<String>(Arrays.asList(textSplitB));

                List<String> wordsListTmp = new ArrayList<>(wordsListA);
                //all elements in A which are not in B
                wordsListTmp.removeAll(wordsListB);
                
                String hashtagToAdd = wordsListTmp.get(0);
                int index = wordsListA.indexOf(hashtagToAdd);
                wordsListB.set(index, "#" + hashtagToAdd);

                String newText = String.join(" ", wordsListB);
                jsonB.put("text", newText);
            }

            JSONArray hashtagsArrayA = retrieveHashtags(jsonA);
            JSONArray hashtagsArrayB = retrieveHashtags(jsonB);

            //merge hashtags
            if(hashtagsArrayA != null && hashtagsArrayB != null){
                List<String> hashtagsListB = new ArrayList();
                for(int i = 0; i < hashtagsArrayB.length(); i++){
                    String hashtag = hashtagsArrayB.getJSONObject(i).getString("text");
                    hashtagsListB.add(hashtag);
                }

                for(int i = 0; i < hashtagsArrayA.length(); i++){
                    String hashtag = hashtagsArrayA.getJSONObject(i).getString("text");
                    if(!hashtagsListB.contains(hashtag)){
                        JSONObject hashtagJsonObject =  new JSONObject();
                        hashtagJsonObject.put("text", hashtag);
                        hashtagsArrayB.put(hashtagJsonObject);
                    }
                }
            }
        }

        return jsonB.toString();
    }
    
    public static void runJob(JavaSparkContext context, JavaRDD<String> data, int k){

        JavaPairRDD<String, Integer> hashtagsTweets = data
            .flatMapToPair(line -> extractHashtagsFromLine(line))
            .distinct();

        JavaPairRDD<String, String> wordsFromTextAndTweet = data
            .flatMapToPair(line -> extractWordsFromText(line));

        JavaPairRDD<String, Tuple2<Integer, String>> hashtagsJoinedTweets = hashtagsTweets.join(wordsFromTextAndTweet);

        JavaPairRDD<Long, String> tweetIDAndTweetModified = hashtagsJoinedTweets
            .mapToPair((Tuple2<String, Tuple2<Integer, String>> tuple) -> replaceKeyByTweetIdAndModifyTweet(tuple))
            .reduceByKey((a,b) -> mergeTweets(a, b));

        System.out.println(tweetIDAndTweetModified.take(10));
    }
}