package storm.blueprints.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class ReportBolt extends BaseRichBolt{
    private OutputCollector collector;
    private HashMap<String, Long> counts;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
        this.collector.ack(tuple);
    }

    public void cleanup(){
        for(String word : this.counts.keySet()){
            System.out.println(word + ": " + counts.get(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
