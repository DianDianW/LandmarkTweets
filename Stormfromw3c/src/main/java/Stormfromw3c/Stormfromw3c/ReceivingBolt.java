package Stormfromw3c.Stormfromw3c;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.JSONObject;

public class ReceivingBolt implements IRichBolt{
	private OutputCollector collector;
	//static JSONObject jsonObj;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String str=input.getString(0);
		
		if(str.toLowerCase().contains("great ocean road")||str.toLowerCase().contains(" gor ")||str.toLowerCase().contains("(gor)")) {
			System.out.println("write GOR success!!!!!!!!!");
			//collector.emit("GORline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"GOR");
		}
		else if(str.toLowerCase().contains("melbourne zoo")) {
			System.out.println("write MZ success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"MZ");
		}
		else if(str.toLowerCase().contains("state library")) {
			System.out.println("write SLoV success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"SLoV");
		}
		else if(str.toLowerCase().contains("bells beach")) {
			System.out.println("write BB success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"BB");
		}
		else if(str.toLowerCase().contains("st kilda")) {
			System.out.println("write StKB success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"StKB");
		}
		else if(str.toLowerCase().contains("yarra river")) {
			System.out.println("write YR success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"YR");
		}
		else if(str.toLowerCase().contains("qeen victoria market")||str.toLowerCase().contains(" qvm ")||str.toLowerCase().contains("(qvm)")) {
			System.out.println("write QVM success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"QVM");
		}
		else if(str.toLowerCase().contains("melbourne cricket ground")||str.toLowerCase().contains(" mcg ")||str.toLowerCase().contains("(mcg)")) {
			System.out.println("write MCG success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"MCG");
		}
		else if(str.toLowerCase().contains("flinders street station")||str.toLowerCase().contains("flinders station")||str.toLowerCase().contains("flinders street railway")) {
			System.out.println("write FSS success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"FSS");
		}
		else if(str.toLowerCase().contains("melbourne museum")) {
			System.out.println("write MM success!!!!!!!!!");
			//collector.emit("MZline",new Values(str));
			WriteJsonFile.WriteConfigJson(str,"MM");
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
