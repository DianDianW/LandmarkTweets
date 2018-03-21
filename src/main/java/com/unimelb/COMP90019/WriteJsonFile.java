package com.unimelb.COMP90019;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteJsonFile {
    public static void WriteConfigJson(String args) {
        SearchTweets searchTweets = new SearchTweets();
//    	searchTweets.setKeyword("great");
        String src = "E:\\"+"location"+".json";//这里需要定义一个变量，如"E:\\json\\conf.json";//把json文件写到这个目录下

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