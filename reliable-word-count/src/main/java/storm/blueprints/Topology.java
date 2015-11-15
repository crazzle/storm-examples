package storm.blueprints;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.blueprints.bolts.ReportBolt;
import storm.blueprints.bolts.SplitSentenceBolt;
import storm.blueprints.bolts.WordCountBolt;
import storm.blueprints.spouts.SentenceSpout;

public class Topology
{
    private static final String SENTENCE_SPOUT_ID = "sentence-spout-id";
    private static final String SPLIT_BOLT_ID = "split-bolt-id";
    private static final String COUNT_BOLT_ID = "count-bolt-id";
    private static final String REPORT_BOLT_ID = "report-bolt-id";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main( String[] args )
    {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 8);
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 4).setNumTasks(8).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt, 8).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        Config  config = new Config();
        config.setNumWorkers(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
