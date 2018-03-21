package com.unimelb.COMP90019;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

//import java.io.File;
//import java.io.FileWriter;
import java.util.HashMap;
//import java.util.List;
import java.util.Map;

//import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.7
 */
public class SearchTweets {

    public final static String keywords = "good";
    /**
     * Usage: java twitter4j.examples.search.SearchTweets [query]
     *
     * @param args
     *            search query
     * @throws JSONException
     */
    //static double[][] boundingBox = {{-55.3228175,-9.0882278},{72.2460938,168.2249543}};//au
    static double[][] boundingBox = {{103.618248,1.1158},{104.40847, 1.47062}};//sing
    static FilterQuery keywordfilter = new FilterQuery();
    static FilterQuery boundfilter = new FilterQuery();

    static WriteFileThread writeFileThread = new WriteFileThread();
    public static void main(String[] args) throws JSONException {

        writeFileThread.start();

        SearchTweets searchTweets = new SearchTweets();

        Twitter twitter = new TwitterFactory().getInstance();

        StatusListener listener = new StatusListener() {
            // @Override
            public void onStatus(Status tweet) {

                //int reTweetCount = tweet.getRetweetCount();
                JSONObject jsonObj = new JSONObject();
                try {
                    jsonObj.put("text", tweet.getText());
                    //jsonObj.put("contributor", "null");
                    jsonObj.put("CreatedAt", tweet.getCreatedAt());
                    if (tweet.getGeoLocation() != null) {
                        Map<String, String> geoLocation = new HashMap<String, String>();
                        geoLocation.put("type", "point");
                        geoLocation.put("coordinates", tweet.getGeoLocation().toString());
                        jsonObj.put("geo", geoLocation);
                    } else if (tweet.getPlace() != null) {
                        jsonObj.put("geo", "null");
                        Map<String, String> place = new HashMap<String, String>();
                        place.put("type", "polygon");
                        place.put("coordinates", tweet.getPlace().toString());
                        jsonObj.put("place", place);
                    } else {
                        jsonObj.put("geo", "null");
                        jsonObj.put("place","null");
                    }
                } catch (twitter4j.JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                StringBuffer jsonString = new StringBuffer().append(jsonObj).append("\r\n");
                //System.out.print(jsonString.toString());
                //jsonString.append("\r\n");
                writeFileThread.setStringJson(jsonString.toString());
            }

            // @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            // @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            // @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            // @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            // @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("0kzHxdlmuE3RNu8YUL7wOYklG");
        cb.setOAuthConsumerSecret("Q0fV8FnECFkhHN8pZgxyKGX2YDtrxtoKtsbHpFUsr1fuk65MRz");
        cb.setOAuthAccessToken("970228986386137088-eWp1RptsUuP996GcZYrmBt27RGdHlvr");
        cb.setOAuthAccessTokenSecret("qQKUvq4CLxCfrRHsLzJOLmkhb6gDQvc6k5qHiGBRHhcGU");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);


//		String keywords[] = {"great", "ocean"};
//		System.out.println(keywords[0]);

//        keywordfilter.track(keywords);
//        twitterStream.filter(keywordfilter);


        boundfilter.locations(boundingBox);
        twitterStream.filter(boundfilter);
    }
}
