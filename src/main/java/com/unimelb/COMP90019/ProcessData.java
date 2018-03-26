package com.unimelb.COMP90019;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
public class ProcessData {
	private static File file;
	private static FileWriter fw;
	private static BufferedWriter bw;
	private static String path = "e:\\yaner\\res.txt";
	public static void writefile() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	file =new File(path);
    //File file = new File(path);
    if(!file.exists()){
        file.getParentFile().mkdirs();          
    }
    try {
		file.createNewFile();
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	try {
		fw = new FileWriter(file, true);
		bw = new BufferedWriter(fw);
	} catch (IOException e111) {
		// TODO Auto-generated catch block
		e111.printStackTrace();
	}
	/*try {
		BufferedReader br = new BufferedReader(new FileReader(  
				"E:\\victoria.json"));
		String s="";
		while ((s = br.readLine()) != null) {
			try {  
				JSONObject dataJson = new JSONObject(s);
				String text= dataJson.get("text").toString();
				findName(text);
		} catch (JSONException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
        } 
		//SearchTweets.findName(tmpstr);
		}
		}catch (IOException e11) {
		// TODO Auto-generated catch block
		e11.printStackTrace();
	}*/
	/*String text;
	try {
		text = Listener.jsonObj.get("text").toString();
		ProcessData.findName(text);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/
    /*try {
    	bw.close();
		fw.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/
	}
	
	public static void findName(String str) throws IOException {
		InputStream modelInToken = null;
		
		modelInToken =new FileInputStream("binfile/da-token.bin");
		TokenizerModel modelToken = new TokenizerModel(modelInToken);
		Tokenizer tokenizer = new TokenizerME(modelToken);
		String tokens[] = tokenizer.tokenize(str);
		
		InputStream modeIn = new FileInputStream("binfile/en-ner-location.bin");
		TokenNameFinderModel model = new TokenNameFinderModel(modeIn);
		//modeIn.close();
		NameFinderME nameFinder = new NameFinderME(model);
			Span nameSpans[] = nameFinder.find(tokens);
			double[] spanProbs = nameFinder.probs(nameSpans);
			for( int i = 0; i<nameSpans.length; i++) {
	    		System.out.println("Span: "+nameSpans[i].toString());
	    		String writeres="";
	    		for(int j=nameSpans[i].getStart();j<nameSpans[i].getEnd();j++) {
	    			writeres=writeres+" "+tokens[j];
	    		}
	    		System.out.println("Covered text is: "+writeres);
	    		System.out.println("Probability is: "+spanProbs[i]);
	            bw.write(writeres+"\r\n");
	            bw.flush();
	    	}		
			
	}
}
