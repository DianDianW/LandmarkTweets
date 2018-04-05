import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * 创建数据源
 */
public class CreateSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = null; //为数据的总容量
    String pathname;
    File filename;
    InputStreamReader reader;
    BufferedReader br;

    private String read() {

        try {
            String line = "";
            line = br.readLine();
            if (line != null) {
                return line;
            } else {
                Utils.sleep(200000000);
            }
        } catch (Exception e) {
        }
        return "null";
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            pathname = "E:\\res.txt";
            filename = new File(pathname);
            reader = new InputStreamReader(
                    new FileInputStream(filename));
            br = new BufferedReader(reader);

        } catch (Exception e) {

        }
    }

    @Override
    public void nextTuple() {
        sentences = new String[]{read()};
        /*storm会循环的调用这个方法*/
        /*线程进行休眠,10s发送一次数据,在这10s内,让其余工作进行*/
        Utils.sleep(100);
        //获得数据源
        System.out.println(new DateTime().toString("HH:mm:ss") + "--------------CreateSpout 开始发送数据----------");
        this.collector.emit(new Values(sentences));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

}