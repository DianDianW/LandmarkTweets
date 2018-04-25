package Stormfromw3c.Stormfromw3c;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteJsonFile {
	
    public static void WriteConfigJson(String args,String key) {
        //TwitterReader searchTweets = new TwitterReader();?????????????????????????????????????/????????????????????????????
//    	searchTweets.setKeyword("great");
    	String src;
        src = "src\\resource\\"+key+".json";//杩欓噷闇�瑕佸畾涔変竴涓彉閲忥紝濡�"E:\\json\\conf.json";//鎶妀son鏂囦欢鍐欏埌杩欎釜鐩綍涓�

        File file = new File(src);
        
        

        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
        	if(key.equals("Result")) {
        		if(file.exists()) {
        			file.delete();
        			file.createNewFile();
        		}
        		else {
        			file.createNewFile();
        		}
        	}
            if(!file.exists()){
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileWriter fw = new FileWriter(file, true);
            fw.write(args);
//            fw.newline();
//            fw.write("\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}