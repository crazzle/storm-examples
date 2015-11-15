package storm.blueprints.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import clojure.lang.Obj;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout extends BaseRichSpout{


    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sentences = new String[]{
            "this is a test",
            "a test is really helpful",
            "some people are helpful too",
            "but not everybody"
    };
    int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.collector.emit(values, msgId);
        ++index;
        if(index >= sentences.length){
            index = 0;
        }
        Utils.sleep(1l);
    }

    public void ack(Object msgId){
        this.pending.remove(msgId);
    }

    public void fail(Object msgId){
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}
