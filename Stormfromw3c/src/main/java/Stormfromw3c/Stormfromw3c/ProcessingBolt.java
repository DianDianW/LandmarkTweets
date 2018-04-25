package Stormfromw3c.Stormfromw3c;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;

public class ProcessingBolt implements IRichBolt {
	private OutputCollector collector;
	private  static int [] Results = new int [11];
	private  static ArrayList ScoresBB = new ArrayList();
	private  static ArrayList ScoresGOR = new ArrayList();
	private  static ArrayList ScoresMZ = new ArrayList();
	private  static ArrayList ScoresMCG = new ArrayList();
	private  static ArrayList ScoresQVM = new ArrayList();
	private  static ArrayList ScoresSLoV = new ArrayList();
	private  static ArrayList ScoresStKB = new ArrayList();
	private  static ArrayList ScoresYR = new ArrayList();
	private  static ArrayList ScoresFSS = new ArrayList();
	private  static ArrayList ScoresMM = new ArrayList();
	private  static ArrayList ScoresRBG = new ArrayList();
	private  static ArrayList BestBB = new ArrayList();
	private  static ArrayList BestGOR = new ArrayList();
	private  static ArrayList BestMZ = new ArrayList();
	private  static ArrayList BestMCG = new ArrayList();
	private  static ArrayList BestQVM = new ArrayList();
	private  static ArrayList BestSLoV = new ArrayList();
	private  static ArrayList BestStKB = new ArrayList();
	private  static ArrayList BestYR = new ArrayList();
	private  static ArrayList BestFSS = new ArrayList();
	private  static ArrayList BestMM = new ArrayList();
	private  static ArrayList BestRBG = new ArrayList();
	 //String [] BestComments = new String [8];
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String sentence = input.getString(0);
		JSONObject dataJson = new JSONObject(sentence);
		String textcontent=dataJson.getString("text");
		NLP.init();
		if(input.getSourceStreamId().equals("jsonline")) {
			//System.out.println(CleanupTweets(textcontent));
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresBB.add(s);
			if(s>=4&&BestBB.size()<=3) {
				BestBB.add(textcontent);
			}
			Results[0]=Results[0]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-GOR")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresGOR.add(s);
			if(s>=3&&BestGOR.size()<=3) {
				BestGOR.add(textcontent);
			}
			//System.out.println(GORScoring(textcontent));
			Results[1]=Results[1]+s;
			
		}
		else if(input.getSourceStreamId().equals("jsonline-MZ")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresMZ.add(s);
			if(s>=3&&BestMZ.size()<=3) {
				BestMZ.add(textcontent);
			}
			Results[2]=Results[2]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-MCG")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresMCG.add(s);
			if(s>=3&&BestMCG.size()<=3) {
				BestMCG.add(textcontent);
			}
			Results[3]=Results[3]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-QVM")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresQVM.add(s);
			if(s>=3&&BestQVM.size()<=3) {
				BestQVM.add(textcontent);
			}
			Results[4]=Results[4]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-SLoV")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresSLoV.add(s);
			if(s>=3&&BestSLoV.size()<=3) {
				BestSLoV.add(textcontent);
			}
			Results[5]=Results[5]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-StKB")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresStKB.add(s);
			if(s>=3&&BestStKB.size()<=3) {
				BestStKB.add(textcontent);
			}
			Results[6]=Results[6]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-YR")) {
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresYR.add(s);
			if(s>=3&&BestYR.size()<=3) {
				BestYR.add(textcontent);
			}
			Results[7]=Results[7]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-FSS")) {
			//System.out.println(CleanupTweets(textcontent));
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresFSS.add(s);
			if(s>=3&&BestFSS.size()<=3) {
				BestFSS.add(textcontent);
			}
			Results[8]=Results[8]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-MM")) {
			//System.out.println(CleanupTweets(textcontent));
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresMM.add(s);
			if(s>=3&&BestMM.size()<=3) {
				BestMM.add(textcontent);
			}
			Results[9]=Results[9]+s;
		}
		else if(input.getSourceStreamId().equals("jsonline-RBG")) {
			//System.out.println(CleanupTweets(textcontent));
			int s=NLP.findSentiment(CleanupTweets(textcontent));
			ScoresRBG.add(s);
			if(s>=3&&BestRBG.size()<=3) {
				BestRBG.add(textcontent);
			}
			Results[10]=Results[10]+s;
		}
		
		/*Landmark BB = new Landmark("Bells Beach",Results[0]);
		Landmark GOR = new Landmark("Great Ocean Road",Results[1]);
		System.out.println("GOR:"+Results[1]+" "+ScoresGOR.size());
		Landmark MZ = new Landmark("Melbourne Zoo",Results[2]);
		System.out.println("MZ:"+Results[2]+" "+ScoresMZ.size());
		Landmark MCG = new Landmark("Melbourne Cricket Ground",Results[3]);
		Landmark QVM = new Landmark("Qeen Victoria Market",Results[4]);
		Landmark SLoV = new Landmark("State Library of Victoria",Results[5]);
		Landmark StKB = new Landmark("St Kilda Beach",Results[6]);
		Landmark YR = new Landmark("Yarra River",Results[7]);
		Landmark FSS = new Landmark("Flinders Street Station",Results[8]);
		Landmark [] resarray = new Landmark [9];
		resarray[0]=BB;resarray[1]=GOR;resarray[2]=MZ;resarray[3]=MCG;resarray[4]=QVM;resarray[5]=SLoV;resarray[6]=StKB;resarray[7]=YR;resarray[8]=FSS;
		for(int i=0;i<8;i++) {
			for(int j=i+1;j<9;j++) {
        		if(resarray[j].score<resarray[i].score) {
        			Landmark temp = new Landmark(resarray[i].name,resarray[i].score);
        			resarray[i]=null;
        			resarray[i]=resarray[j];
        			resarray[j]=null;
        			resarray[j]=temp;
        		}
        	}
		}
		String resstring="";
		for(int i=8;i>=0;i--) {
        	System.out.println((9-i)+". "+resarray[i].name+": "+resarray[i].score);
        	resstring=resstring+(9-i)+". "+resarray[i].name+": "+resarray[i].score+"\r\n";
        }
		WriteJsonFile.WriteConfigJson(resstring, "Result");*/
		if(ScoresBB.size()!=0&&ScoresGOR.size()!=0&&ScoresMZ.size()!=0&&ScoresMCG.size()!=0&&ScoresQVM.size()!=0&&ScoresSLoV.size()!=0&&ScoresStKB.size()!=0&&ScoresYR.size()!=0&&ScoresFSS.size()!=0&&ScoresMM.size()!=0&&ScoresRBG.size()!=0) {
			double BBmark =(double)Results[0]/(double)ScoresBB.size();
			double GORmark =(double)Results[1]/(double)ScoresGOR.size();
			double MZmark = (double)Results[2]/(double)ScoresMZ.size();
			double MCGmark = (double)Results[3]/(double)ScoresMCG.size();
			double QVMmark = (double)Results[4]/(double)ScoresQVM.size();
			double SLoVmark = (double)Results[5]/(double)ScoresSLoV.size();
			double StKBmark = (double)Results[6]/(double)ScoresStKB.size();
			double YRmark = (double)Results[7]/(double)ScoresYR.size();
			double FSSmark = (double)Results[8]/(double)ScoresFSS.size();
			double MMmark = (double)Results[9]/(double)ScoresMM.size();
			double RBGmark = (double)Results[10]/(double)ScoresRBG.size();
		Landmark BB = new Landmark("Bells Beach",BBmark,BestBB);
		Landmark GOR = new Landmark("Great Ocean Road",GORmark,BestGOR);
		System.out.println("GOR:"+Results[1]+" "+ScoresGOR.size());
		Landmark MZ = new Landmark("Melbourne Zoo",MZmark,BestMZ);
		System.out.println("MZ:"+Results[2]+" "+ScoresMZ.size());
		Landmark MCG = new Landmark("Melbourne Cricket Ground",MCGmark,BestMCG);
		Landmark QVM = new Landmark("Qeen Victoria Market",QVMmark,BestQVM);
		Landmark SLoV = new Landmark("State Library of Victoria",SLoVmark,BestSLoV);
		Landmark StKB = new Landmark("St Kilda Beach",StKBmark,BestStKB);
		Landmark YR = new Landmark("Yarra River",YRmark,BestYR);
		Landmark FSS = new Landmark("Flinders Street Station",FSSmark,BestFSS);
		Landmark MM = new Landmark("Melbourne Museum",MMmark,BestMM);
		Landmark RBG = new Landmark("Royal Botanic Gardens Melbourne",RBGmark,BestRBG);
		Landmark [] resarray = new Landmark [11];
		resarray[0]=BB;resarray[1]=GOR;resarray[2]=MZ;resarray[3]=MCG;resarray[4]=QVM;resarray[5]=SLoV;resarray[6]=StKB;resarray[7]=YR;resarray[8]=FSS;resarray[9]=MM;resarray[10]=RBG;
		
		for(int i=0;i<10;i++) {
			for(int j=i+1;j<11;j++) {
        		if(resarray[j].score<resarray[i].score) {
        			Landmark temp = new Landmark(resarray[i].name,resarray[i].score,resarray[i].Best);
        			resarray[i]=null;
        			resarray[i]=resarray[j];
        			resarray[j]=null;
        			resarray[j]=temp;
        		}
        		
        	}
		}
		String resstring="";
		for(int i=10;i>=0;i--) {
        	System.out.println((11-i)+". "+resarray[i].name+": "+resarray[i].score);
        	resstring=resstring+(11-i)+". "+resarray[i].name+": "+resarray[i].score+"\r\n";
        }
		WriteJsonFile.WriteConfigJson(resstring, "Result");
		String Resline ="";
		for(int i=10;i>=0;i--) {
			Resline=(11-i)+"\r\n";
			for(int j=0;j<resarray[i].Best.size();j++) {
				Resline=Resline+resarray[i].Best.get(j)+"\r\n";
			}
			WriteResFile.WriteResFile(Resline,resarray[i].name);
        }
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		/*Landmark BB = new Landmark("Bells Beach",Results[0]);
		Landmark GOR = new Landmark("Great Ocean Road",Results[1]);
		Landmark MZ = new Landmark("Melbourne Zoo",Results[2]);
		Landmark MCG = new Landmark("Melbourne Cricket Ground",Results[3]);
		Landmark QVM = new Landmark("Qeen Victoria Market",Results[4]);
		Landmark SLoV = new Landmark("State Library of Victoria",Results[5]);
		Landmark StKB = new Landmark("St Kilda Beach",Results[6]);
		Landmark YR = new Landmark("Yarra River",Results[7]);
		Landmark [] resarray = new Landmark [8];
		resarray[0]=BB;resarray[1]=GOR;resarray[2]=MZ;resarray[3]=MCG;resarray[4]=QVM;resarray[5]=SLoV;resarray[6]=StKB;resarray[7]=YR;
		
		for(int i=0;i<7;i++) {
			for(int j=i+1;j<8;j++) {
        		if(resarray[j].score<resarray[i].score) {
        			Landmark temp = new Landmark(resarray[i].name,resarray[i].score);
        			resarray[i]=null;
        			resarray[i]=resarray[j];
        			resarray[j]=null;
        			resarray[j]=temp;
        		}
        		
        	}
		}
		String resstring="";
		for(int i=7;i>=0;i--) {
        	System.out.println((8-i)+". "+resarray[i].name+": "+resarray[i].score);
        	resstring=resstring+(8-i)+". "+resarray[i].name+": "+resarray[i].score+"\r\n";
        }
		WriteJsonFile.WriteConfigJson(resstring, "Result");*/
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String CleanupTweets (String str) {
		String strres="";
		String[] words = str.split(" ");
		for(String word : words) {
			word = word.trim();
			if(!word.isEmpty()) {
				word=word.toLowerCase();
                String pattern = "http.*";//RegExp
                String pattern1=".*[A-Za-z|\\;|\\:|\\)|\\=|\\^|\\-|\\^|\\(|'].*";//RegExp
                String pattern2 = "@.*";//RegExp
                String pattern3 = "#.*";//RegExp
                boolean isMatch = Pattern.matches(pattern, word);
                boolean isMatch1 = Pattern.matches(pattern1, word);
                boolean isMatch2 = Pattern.matches(pattern2, word);
                boolean isMatch3 = Pattern.matches(pattern3, word);
                if(!isMatch&&isMatch1&&!isMatch2&&!isMatch3) //pick up useful words for each tweet
                {
                	if(strres=="") {
                		strres=strres+word; 
                	}
                	else {
                		strres=strres+" "+word;
                	}
                	                
                }
			}
		}
		return strres;
	}
	
	public int Scoring (String str) {
		int score=0;
		String[] words = str.split(" ");
		for(String word : words) {
			word = word.trim();
			if(!word.isEmpty()) {
				word=word.toLowerCase();
                String pattern = "http.*";//RegExp
                String pattern1=".*[A-Za-z|\\;|\\:|\\)|\\=|\\^|\\-|\\^|\\(|'].*";//RegExp
                String pattern2 = "@.*";//RegExp
                String pattern3 = "#.*";//RegExp
                boolean isMatch = Pattern.matches(pattern, word);
                boolean isMatch1 = Pattern.matches(pattern1, word);
                boolean isMatch2 = Pattern.matches(pattern2, word);
                boolean isMatch3 = Pattern.matches(pattern3, word);
                if(!isMatch&&isMatch1&&!isMatch2&&!isMatch3) //pick up useful words for each tweet
                {
                	score=scoring(word,score);                
                }
			}
		}
		return score;
	}
	public int GORScoring (String str) {
		int score=0;
		String[] words = str.split(" ");
		for(String word : words) {
			word = word.trim();
			if(!word.isEmpty()) {
				word=word.toLowerCase();
                String pattern = "http.*";//RegExp
                String pattern1=".*[A-Za-z|\\;|\\:|\\)|\\=|\\^|\\-|\\^|\\(|'].*";//RegExp
                String pattern2 = "@.*";//RegExp
                String pattern3 = "#.*";//RegExp
                boolean isMatch = Pattern.matches(pattern, word);
                boolean isMatch1 = Pattern.matches(pattern1, word);
                boolean isMatch2 = Pattern.matches(pattern2, word);
                boolean isMatch3 = Pattern.matches(pattern3, word);
                if(!isMatch&&isMatch1&&!isMatch2&&!isMatch3) //pick up useful words for each tweet
                {
                	score=gorscoring(word,score);                
                }
			}
		}
		return score;
	}
	public int scoring(String str,int score) {
		if(str.equals("good")||str.equals("beautiful")||str.equals("great")||str.equals("fantastic")
				||str.equals("wonderful")||str.equals("cool")||str.equals("lovely")||str.equals("nice")||str.equals("happy")
				||str.equals("fantastic")||str.equals("awesome")||str.equals("well")||str.equals("welcome")||str.equals("grand")
				||str.equals("marvelous")||str.equals("wondrous")||str.equals("terrific")||str.equals("tremendous")||str.equals("grt")//abbreviations
				||str.equals("gud")||str.equals("lovl")||str.equals("rawr")||str.equals("best")||str.equals("relax")||str.equals("relaxed")
				||str.equals("glory")||str.equals("love")||str.equals("amazing")||str.equals("like")||str.equals("smile")||str.equals("epic")
				||str.equals("glorious")||str.equals("brilliant")) {
				 score++;
			 }
			 if(str.equals("bad")||str.equals("sick")||str.equals("damn")||str.equals("WTF")||str.equals("ugly")
				||str.equals("disappointed")||str.equals("holy")||str.equals("shit")||str.equals("jesus")|| str.equals("fuck")
				||str.equals("crap")||str.equals("shite")||str.equals("bored")||str.equals("boring")||str.equals("suck")) {
				 score--;
			 }
			 if(str.equals(":)")||str.equals(":-)")||str.equals(":D")||str.equals(";p")||str.equals(";-)")||str.equals(";D")
				||str.equals(";)")||str.equals(":p")||str.equals("=)")||str.equals("=]")||str.equals("=D")||str.equals("XD")||str.equals("^-^")
				||str.equals("(;-)")||str.equals("(:-)")||str.equals("(:-*")||str.equals(":))")||str.equals(":-))")||str.equals(":-*")
				||str.equals(":->")||str.equals(":-]")||str.equals(":-{)")||str.equals(":-}")||str.equals(":^D")||str.equals("B-)")
				||str.equals("|-D")){
				 score=score+2;
			 }
			 if(str.equals(":(")||str.equals(":-(")||str.equals(":o")||str.equals(";(")||str.equals(";-(")||str.equals("=(")
				||str.equals(":E")||str.equals(":-E")||str.equals(";E")||str.equals(";-E")||str.equals("(:-\\")||str.equals("(:-(")
				||str.equals("(:-&")||str.equals("(;-&")||str.equals("(:&")||str.equals("(;&")||str.equals("-_-")||str.equals(":[")
				||str.equals(":|")||str.equals(":-/")||str.equals(":-<")||str.equals(":-C")||str.equals(":-[")||str.equals(":-{")
				||str.equals(":/)")||str.equals("T_T")||str.equals("T-T")){
				 score=score-2;
				 }
			 return score;
		
	}
	public int gorscoring(String str,int score) {
		if(str.equals("good")||str.equals("beautiful")||str.equals("fantastic")
				||str.equals("wonderful")||str.equals("cool")||str.equals("lovely")||str.equals("nice")||str.equals("happy")
				||str.equals("fantastic")||str.equals("awesome")||str.equals("well")||str.equals("welcome")||str.equals("grand")
				||str.equals("marvelous")||str.equals("wondrous")||str.equals("terrific")||str.equals("tremendous")||str.equals("grt")//abbreviations
				||str.equals("gud")||str.equals("lovl")||str.equals("rawr")||str.equals("best")||str.equals("relax")||str.equals("relaxed")
				||str.equals("glory")||str.equals("love")||str.equals("amazing")||str.equals("like")||str.equals("smile")||str.equals("epic")
				||str.equals("glorious")||str.equals("brilliant")) {
				 score++;
			 }
			 if(str.equals("bad")||str.equals("sick")||str.equals("damn")||str.equals("WTF")||str.equals("ugly")
				||str.equals("disappointed")||str.equals("holy")||str.equals("shit")||str.equals("jesus")|| str.equals("fuck")
				||str.equals("crap")||str.equals("shite")||str.equals("bored")||str.equals("boring")||str.equals("suck")) {
				 score--;
			 }
			 if(str.equals(":)")||str.equals(":-)")||str.equals(":D")||str.equals(";p")||str.equals(";-)")||str.equals(";D")
				||str.equals(";)")||str.equals(":p")||str.equals("=)")||str.equals("=]")||str.equals("=D")||str.equals("XD")||str.equals("^-^")
				||str.equals("(;-)")||str.equals("(:-)")||str.equals("(:-*")||str.equals(":))")||str.equals(":-))")||str.equals(":-*")
				||str.equals(":->")||str.equals(":-]")||str.equals(":-{)")||str.equals(":-}")||str.equals(":^D")||str.equals("B-)")
				||str.equals("|-D")||str.equals("^^")){
				 score=score+2;
			 }
			 if(str.equals(":(")||str.equals(":-(")||str.equals(":o")||str.equals(";(")||str.equals(";-(")||str.equals("=(")
				||str.equals(":E")||str.equals(":-E")||str.equals(";E")||str.equals(";-E")||str.equals("(:-\\")||str.equals("(:-(")
				||str.equals("(:-&")||str.equals("(;-&")||str.equals("(:&")||str.equals("(;&")||str.equals("-_-")||str.equals(":[")
				||str.equals(":|")||str.equals(":-/")||str.equals(":-<")||str.equals(":-C")||str.equals(":-[")||str.equals(":-{")
				||str.equals(":/)")||str.equals("T_T")||str.equals("T-T")){
				 score=score-2;
				 }
			 return score;
		
	}

}
