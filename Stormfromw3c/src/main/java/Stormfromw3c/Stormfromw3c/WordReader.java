package Stormfromw3c.Stormfromw3c;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader implements IRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader1;
    private FileReader fileReader2;
    private FileReader fileReader3;
    private FileReader fileReader4;
    private FileReader fileReader5;
    private FileReader fileReader6;
    private FileReader fileReader7;
    private FileReader fileReader8;
    private FileReader fileReader9;
    private FileReader fileReader10;
    private FileReader fileReader11;
    private boolean completed = false;
    public boolean isDistributed() {return false;}
    public void ack(Object msgId) {
            System.out.println("OK:"+msgId);
    }
    public void close() {}
    public void fail(Object msgId) {
         System.out.println("FAIL:"+msgId);
    }
    /**
     * 这个方法做的惟一一件事情就是分发文件中的文本行
     */
    public void nextTuple() {
    /**
     * 这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
     */
         if(completed){
             try {
                 Thread.sleep(1000);
                 //System.out.println("Reading finish, sleep for a while");
                 completed = false;
             } catch (InterruptedException e) {
                 //什么也不做
             }
            return;
         }
         String str;
         //创建reader
         BufferedReader reader1 = new BufferedReader(fileReader1);
         BufferedReader reader2 = new BufferedReader(fileReader2);
         BufferedReader reader3 = new BufferedReader(fileReader3);
         BufferedReader reader4 = new BufferedReader(fileReader4);
         BufferedReader reader5 = new BufferedReader(fileReader5);
         BufferedReader reader6 = new BufferedReader(fileReader6);
         BufferedReader reader7 = new BufferedReader(fileReader7);
         BufferedReader reader8 = new BufferedReader(fileReader8);
         BufferedReader reader9 = new BufferedReader(fileReader9);
         BufferedReader reader10 = new BufferedReader(fileReader10);
         BufferedReader reader11 = new BufferedReader(fileReader11);
         try{
             //读所有文本行
            /*while((str = reader1.readLine()) != null){
             /**
              * 按行发布一个新值
              */
                /* this.collector.emit("wordsline",new Values(str));//bind to declareStream function
             }*/
        	 String strRBG;
             while((strRBG = reader11.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-RBG",new Values(strRBG));
                     
                 }
        	 String strMM;
             while((strMM = reader10.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-MM",new Values(strMM));
                     
                 }
        	 String strFSS;
             while((strFSS = reader9.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-FSS",new Values(strFSS));
                     
                 }
        	 String strMCG;
             while((strMCG = reader8.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-MCG",new Values(strMCG));
                     
                 }
        	 String strStKB;
             while((strStKB = reader7.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-StKB",new Values(strStKB));
                     
                 }
        	 String strSLoV;
             while((strSLoV = reader6.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-SLoV",new Values(strSLoV));
                     
                 }
        	 String strMZ;
             while((strMZ = reader5.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-MZ",new Values(strMZ));
                     
                 }
        	 String strYR;
             while((strYR = reader4.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-YR",new Values(strYR));
                     
                 }
        	 String strGOR;
             while((strGOR = reader1.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-GOR",new Values(strGOR));
                     
                 }
             String strQVM;
             while((strQVM = reader3.readLine()) != null){
                 /**
                  * 按行发布一个新值
                  */
                     this.collector.emit("jsonline-QVM",new Values(strQVM));
                 }
            String str1;
            while((str1 = reader2.readLine()) != null){
                /**
                 * 按行发布一个新值
                 */
                    this.collector.emit("jsonline",new Values(str1));
                    //System.out.println("Reading BB.json !!!");
                    //System.out.println("----------From Reader-----------");
                }
         }catch(Exception e){
             throw new RuntimeException("Error reading tuple",e);//bind to declareStream function
         }finally{
             completed = true;
         }
     }
     /**
      * 我们将创建一个文件并维持一个collector对象
      */
     public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
             try {
                 //this.fileReader1 = new FileReader(conf.get("wordsFile").toString());
                 this.fileReader2 = new FileReader(conf.get("jsonFile").toString());
                 this.fileReader1 = new FileReader(conf.get("GOR").toString());
                 this.fileReader3 = new FileReader(conf.get("QVM").toString());
                 this.fileReader4 = new FileReader(conf.get("YR").toString());
                 this.fileReader5 = new FileReader(conf.get("MZ").toString());
                 this.fileReader6 = new FileReader(conf.get("SLoV").toString());
                 this.fileReader7 = new FileReader(conf.get("StKB").toString());
                 this.fileReader8 = new FileReader(conf.get("MCG").toString());
                 this.fileReader9 = new FileReader(conf.get("FSS").toString());
                 this.fileReader10 = new FileReader(conf.get("MM").toString());
                 this.fileReader11 = new FileReader(conf.get("RBG").toString());
             } catch (FileNotFoundException e) {
            	 throw new RuntimeException("Error reading file!!!");
                 //throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
             }
             this.collector = collector;
     }
     /**
      * 声明输入域"word"
      */
     public void declareOutputFields(OutputFieldsDeclarer declarer) {
         declarer.declareStream("jsonline",new Fields("line"));//send multiple files based on streamID
         declarer.declareStream("jsonline-GOR",new Fields("line"));
         declarer.declareStream("jsonline-QVM",new Fields("line"));
         declarer.declareStream("jsonline-YR",new Fields("line"));
         declarer.declareStream("jsonline-MZ",new Fields("line"));
         declarer.declareStream("jsonline-SLoV",new Fields("line"));
         declarer.declareStream("jsonline-StKB",new Fields("line"));
         declarer.declareStream("jsonline-MCG",new Fields("line"));
         declarer.declareStream("jsonline-FSS",new Fields("line"));
         declarer.declareStream("jsonline-MM",new Fields("line"));
         declarer.declareStream("jsonline-RBG",new Fields("line"));
         //declarer.declare(new Fields("linefromjson"));
     }
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
} 
