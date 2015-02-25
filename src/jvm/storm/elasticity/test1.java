package storm.elasticity;

import storm.elasticity.bolt.TestBolt;
import storm.elasticity.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class test1 {
	public static void main(String[] args) throws Exception {
		

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout_head_1", new TestSpout(), 2);
		builder.setBolt("bolt_linear_2", new TestBolt(), 8)
		.shuffleGrouping("spout_head_1");
		builder.setBolt("bolt_output_3", new TestBolt(), 8)
		.shuffleGrouping("bolt_linear_2");
		
		builder.setSpout("spout_head_2", new TestSpout(), 2);
		builder.setBolt("bolt_linear_3", new TestBolt(), 8)
		.shuffleGrouping("spout_head_2");
		builder.setBolt("bolt_output_4", new TestBolt(), 8)
		.shuffleGrouping("bolt_linear_3");

		

		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);

		conf.setNumAckers(0);

		conf.setNumWorkers(4);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
