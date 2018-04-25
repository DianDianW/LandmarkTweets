package Stormfromw3c.Stormfromw3c;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
    //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Twitter-reader", new TwitterReader());
        BoltDeclarer bd = builder.setBolt("Twitter-Writer", new ReceivingBolt(), 1);
        bd.shuffleGrouping("Twitter-reader");
        builder.setSpout("Data-reader", new WordReader());
        BoltDeclarer PB = builder.setBolt("Data-Processer", new ProcessingBolt(), 8);
        PB.fieldsGrouping("Data-reader","jsonline", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-GOR", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-QVM", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-YR", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-MZ", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-SLoV", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-StKB", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-MCG", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-FSS", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-MM", new Fields("line"));
        PB.fieldsGrouping("Data-reader","jsonline-RBG", new Fields("line"));
        /*builder.setSpout("word-reader", new WordReader());--------------------------------
        BoltDeclarer bd = builder.setBolt("json-normalizer", new JsonNormalizer(), 1); 
        bd.fieldsGrouping("word-reader","jsonline", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-GOR", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-QVM", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-YR", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-MZ", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-SLoV", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-StKB", new Fields("line"));
        bd.fieldsGrouping("word-reader","jsonline-MCG", new Fields("line"));
        
        //bd.fieldsGrouping("word-reader",new Fields("line"));
        ///bd.shuffleGrouping("word-reader");
        //bd.fieldsGrouping("word-reader",new Fields("line"));
        BoltDeclarer counter = builder.setBolt("jsonscoring", new JsonScoring(), 1); 
        counter.fieldsGrouping("json-normalizer","BBToken",new Fields("bbword"));
        counter.fieldsGrouping("json-normalizer","GORToken",new Fields("gorword"));
        counter.fieldsGrouping("json-normalizer","QVMToken",new Fields("qvmword"));
        counter.fieldsGrouping("json-normalizer","YRToken",new Fields("yrword"));
        counter.fieldsGrouping("json-normalizer","MZToken",new Fields("mzword"));
        counter.fieldsGrouping("json-normalizer","SLoVToken",new Fields("slovword"));
        counter.fieldsGrouping("json-normalizer","StKBToken",new Fields("stkbword"));
        counter.fieldsGrouping("json-normalizer","MCGToken",new Fields("mcgword"));---------------*/
        //builder.setBolt("json-normalizer", new JsonNormalizer()).shuffleGrouping("word-reader");
        //builder.setBolt("jsonscoringres", new JsonScoring()).shuffleGrouping("json-normalizer");
       // builder.setBolt("jsonnormalizer", new JsonNormalizer()).allGrouping(componentId, streamId);
        //builder.setBolt("jsonScoring", new JsonScoring()).globalGrouping("jsonnormalizer");

    //配置
        Config conf = new Config();
        //conf.put("wordsFile", args[0]);
        conf.put("jsonFile", args[0]);
        conf.put("GOR", args[1]);
        conf.put("QVM", args[2]);
        conf.put("YR", args[3]);
        conf.put("MZ", args[4]);
        conf.put("SLoV", args[5]);
        conf.put("StKB", args[6]);
        conf.put("MCG", args[7]);
        conf.put("FSS", args[8]);
        conf.put("MM", args[9]);
        conf.put("RBG", args[10]);
        conf.setDebug(false);

    //运行拓扑
         conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        //Utils.sleep(20000);
        //cluster.shutdown();
    }
}  
