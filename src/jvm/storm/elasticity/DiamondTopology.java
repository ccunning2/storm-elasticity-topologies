package storm.elasticity;

import storm.elasticity.bolt.TestBolt;
import storm.elasticity.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class DiamondTopology {
	public static void main(String[] args) throws Exception {
		//int paralellism = 15;

        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("spout_head", new TestSpout(), 10).setNumTasks(4);
        builder.setSpout("spout_head", new TestSpout(), 10);

//        builder.setBolt("bolt_1", new TestBolt(), 5).setNumTasks(12).shuffleGrouping("spout_head");
//        builder.setBolt("bolt_2", new TestBolt(), 5).setNumTasks(12).shuffleGrouping("spout_head");
//        builder.setBolt("bolt_3", new TestBolt(), 5).setNumTasks(12).shuffleGrouping("spout_head");
//        builder.setBolt("bolt_4", new TestBolt(), 5).setNumTasks(12).shuffleGrouping("spout_head");
        builder.setBolt("bolt_1", new TestBolt(), 5).shuffleGrouping("spout_head");
	    builder.setBolt("bolt_2", new TestBolt(), 5).shuffleGrouping("spout_head");
	    builder.setBolt("bolt_3", new TestBolt(), 5).shuffleGrouping("spout_head");
	    builder.setBolt("bolt_4", new TestBolt(), 5).shuffleGrouping("spout_head");

        //BoltDeclarer output = builder.setBolt("bolt_output_3", new TestBolt(), 20).setNumTasks(12);
	    BoltDeclarer output = builder.setBolt("bolt_output_3", new TestBolt(), 20);
        output.shuffleGrouping("bolt_1");
        output.shuffleGrouping("bolt_2");
        output.shuffleGrouping("bolt_3");
        output.shuffleGrouping("bolt_4");

        Config conf = new Config();
        conf.setDebug(true);

         conf.setNumAckers(0);

        conf.setNumWorkers(12);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        builder.createTopology());

	}

}