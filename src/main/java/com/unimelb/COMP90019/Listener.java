package com.unimelb.COMP90019;
/**
 * Usage: java twitter4j.examples.search.SearchTweets [query]
 *
 * @param args
 * search query
 * @throws JSONException
 */
import twitter4j.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Listener {
	static JSONObject jsonObj;
    static StatusListener listener = new StatusListener() {
        // @Override
        public void onStatus(Status tweet) {

            //int reTweetCount = tweet.getRetweetCount();
            jsonObj = new JSONObject();
            if (!SearchTweets.keywordflag||tweet.getText().toString().contains(SearchTweets.keywords)) {//match phrase with multi-words
                try {
                    jsonObj.put("text", tweet.getText());
                    jsonObj.put("id",tweet.getId());
                    //jsonObj.put("contributor", "null");
                    //System.out.println(tweet.getText());
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
                        jsonObj.put("place", "null");
                    }
                } catch (twitter4j.JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                StringBuffer jsonString = new StringBuffer().append(jsonObj).append("\r\n");
                //System.out.print(jsonString.toString());

                SearchTweets.writeFileThread.setStringJson(jsonString.toString());
            }
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
}
