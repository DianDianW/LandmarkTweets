package com.unimelb.COMP90019;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteJsonFile {
	public static String src;
    public static void WriteConfigJson(String args) {
        SearchTweets searchTweets = new SearchTweets();
//    	searchTweets.setKeyword("great");
        src = "E:\\"+"melbourne"+".json";//杩欓噷闇�瑕佸畾涔変竴涓彉閲忥紝濡�"E:\\json\\conf.json";//鎶妀son鏂囦欢鍐欏埌杩欎釜鐩綍涓�

        File file = new File(src);

        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
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