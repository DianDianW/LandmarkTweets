package Stormfromw3c.Stormfromw3c;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
public class JsonNormalizer implements IRichBolt{
	private OutputCollector collector;
	private TopologyContext context;
	private boolean flag=false;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		this.context = context;
		
	}

	public void execute(Tuple input) {
		//System.out.println(input.getSourceGlobalStreamId());
		String sentence = input.getString(0);
		JSONObject dataJson = new JSONObject(sentence);
		String textcontent=dataJson.getString("text");
		String[] words = textcontent.split(" ");
		//process sentences? or process words by words...
		for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){//add pre-processing here like no numbers no URL etc.
            	if(flag==true) {
            		word="";
            		flag=false;
            	}
                word=word.toLowerCase();
                String pattern = "http.*";//RegExp
                String pattern1=".*[A-Za-z|\\;|\\:|\\)|\\=|\\^|\\-|\\^|\\(|'].*";//RegExp
                String pattern2 = "@.*";//RegExp
                String pattern3 = "#.*";//RegExp
                boolean isMatch = Pattern.matches(pattern, word);
                boolean isMatch1 = Pattern.matches(pattern1, word);
                boolean isMatch2 = Pattern.matches(pattern2, word);
                boolean isMatch3 = Pattern.matches(pattern3, word);
                if(!isMatch&&isMatch1&&!isMatch2&&!isMatch3) //no URL, @... and hash tags 
                {
                	if(word.equals("don't")||word.equals("didn't")||word.equals("not")||word.equals("isn't")) {
                		flag=true;
                	}
                	if(input.getSourceStreamId().equals("jsonline")) {
                		collector.emit("BBToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-GOR")) {
                		collector.emit("GORToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-QVM")) {
                		collector.emit("QVMToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-YR")) {
                		collector.emit("YRToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-MZ")) {
                		collector.emit("MZToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-SLoV")) {
                		collector.emit("SLoVToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-StKB")) {
                		collector.emit("StKBToken",new Values(word));
                	}
                	else if(input.getSourceStreamId().equals("jsonline-MCG")) {
                		collector.emit("MCGToken",new Values(word));
                	}
                	
                }
	}
	}
		collector.ack(input);
		}
        //对元组做出应答
        
        //String[] words = sentence.split(" ");
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		declarer.declareStream("BBToken",new Fields("bbword"));//send multiple files based on streamID
        declarer.declareStream("GORToken",new Fields("gorword"));
        declarer.declareStream("QVMToken",new Fields("qvmword"));
        declarer.declareStream("YRToken",new Fields("yrword"));
        declarer.declareStream("MZToken",new Fields("mzword"));
        declarer.declareStream("SLoVToken",new Fields("slovword"));
        declarer.declareStream("StKBToken",new Fields("stkbword"));
        declarer.declareStream("MCGToken",new Fields("mcgword"));
		// TODO Auto-generated method stub
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
