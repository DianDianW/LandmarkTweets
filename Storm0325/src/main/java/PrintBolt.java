import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;

import java.util.*;

public class PrintBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {

        System.out.println(new DateTime().toString("HH:mm:ss") + "--------------------final bolt 开始运行--------------------");
        Map<String, Integer> counts = (Map<String, Integer>) input.getValue(0);
        /*最后一个阶段,可以持久到mysql等数据库中*/
        System.out.println(justForm(20 - 8) + "key" + justForm(20 - 8) + "      " + "value");
        Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String,Integer>>() {

            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
// TODO Auto-generated method stub
                return o1.getValue()-o2.getValue();
            }
        };
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String,Integer>>(counts.entrySet());
        Collections.sort(list,valueComparator);
        try {
            System.out.println("/////////////////////////////////////////////////////////");//与之前的打印信息分隔,直观的得到结果

            for (Map.Entry<String, Integer> kv : list) { /*这里的justForm()函数是为了保证格式一致*/
                System.out.println(kv.getKey() + justForm(kv.getKey().length()) + " 频数 : " + kv.getValue());
//            }
            }
        } catch (Exception e) {
            System.out.println("/////////////////////////////////////////////////////////////////////得到一个错误的值");
        }
        try {
            Utils.sleep(60000);
        }catch (Exception e){

        }

    }

    private String justForm(int length) {
        for (int i = 0; i < 20 - length; i++) {
            System.out.print(" ");
        }
        return "";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}