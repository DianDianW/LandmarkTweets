package Stormfromw3c.Stormfromw3c;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterReader implements IRichSpout {
	//public static String keywords = lmlist(0);
	private SpoutOutputCollector collector;

    static double[][] boundingBox = new double[2][2];
    static boolean keywordflag = false;
    static FilterQuery keywordfilter = new FilterQuery();
    static FilterQuery boundfilter = new FilterQuery();
    
    static WriteFileThread writeFileThread = new WriteFileThread();

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		// TODO Auto-generated method stub
		writeFileThread.start();
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("0kzHxdlmuE3RNu8YUL7wOYklG");
        cb.setOAuthConsumerSecret("Q0fV8FnECFkhHN8pZgxyKGX2YDtrxtoKtsbHpFUsr1fuk65MRz");
        cb.setOAuthAccessToken("970228986386137088-eWp1RptsUuP996GcZYrmBt27RGdHlvr");
        cb.setOAuthAccessTokenSecret("qQKUvq4CLxCfrRHsLzJOLmkhb6gDQvc6k5qHiGBRHhcGU");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(Listener.listener);
        
        if (!keywordflag) {
            
            Map<String, Double> coords;
            coords = OpenStreetMapUtils.getInstance().getCoordinates("Victoria Australia");
            boundingBox[0][0] = coords.get("latmin");
            boundingBox[0][1] = coords.get("lonmin");
            boundingBox[1][0] = coords.get("latmax");
            boundingBox[1][1] = coords.get("lonmax");
            boundfilter.locations(boundingBox);
            twitterStream.filter(boundfilter);

        } else {
            //keywordfilter.track(keywords);
            //twitterStream.filter(keywordfilter);
        }
		
	}
	
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(Listener.linefromTwitter==true) {
			this.collector.emit(new Values(Listener.linecontent));
			System.out.println("Emit Success!"+Listener.linecontent);
			Listener.linefromTwitter=false;
		}
		
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("OK:"+msgId);
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("FAIL:"+msgId);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("linesfromTwitter"));
		
	}
	
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
