package storm.elasticity;

import storm.elasticity.bolt.TestBolt;
import storm.elasticity.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class StarTopology {
	public static void main(String[] args) throws Exception {
		int numSpout = 2;
		int numBolt = 2;
		int paralellism = 2;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new TestBolt(),
				paralellism*2);

		for (int i = 0; i < numSpout; i++) {
			builder.setSpout("spout_" + i, new TestSpout(), paralellism);
			center.shuffleGrouping("spout_" + i);
		}

		for (int i = 0; i < numBolt; i++) {
			builder.setBolt("bolt_output_" + i, new TestBolt(), paralellism)
					.shuffleGrouping("center");
		}
		
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_DEBUG, false);
		
		conf.setNumAckers(0);

		conf.setNumWorkers(16);
		
	

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}
}
