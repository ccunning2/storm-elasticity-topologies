package storm.elasticity;

import storm.elasticity.bolt.AggregationBolt;
import storm.elasticity.bolt.FilterBolt;
import storm.elasticity.bolt.TestBolt;
import storm.elasticity.bolt.TransformBolt;
import storm.elasticity.spout.RandomLogSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class PageLoadTopology {
	public static void main(String[] args) throws Exception {
		//int numBolt = 3;
		int paralellism = 2;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout_head", new RandomLogSpout(), paralellism);

		/*for (int i = 0; i < numBolt; i++) {
			if (i == 0) {
				builder.setBolt("bolt_linear_" + i, new TestBolt(), paralellism)
						.shuffleGrouping("spout_head");
			} else {
				if (i == (numBolt - 1)) {
					builder.setBolt("bolt_output_" + i, new TestBolt(),
							paralellism).shuffleGrouping(
							"bolt_linear_" + (i - 1));
				} else {
					builder.setBolt("bolt_linear_" + i, new TestBolt(),
							paralellism).shuffleGrouping(
							"bolt_linear_" + (i - 1));
				}
			}
		}*/
		builder.setBolt("bolt_transform", new TransformBolt(), paralellism).shuffleGrouping("spout_head").setNumTasks(10);
		builder.setBolt("bolt_filter", new FilterBolt(), paralellism).shuffleGrouping("bolt_transform").setNumTasks(10);
		builder.setBolt("bolt_join", new TestBolt(), paralellism).shuffleGrouping("bolt_filter").setNumTasks(10);
		builder.setBolt("bolt_filter_2", new FilterBolt(), paralellism).shuffleGrouping("bolt_join").setNumTasks(10);
		builder.setBolt("bolt_aggregate", new AggregationBolt(), paralellism).shuffleGrouping("bolt_filter_2").setNumTasks(10);
		builder.setBolt("bolt_output_sink", new TestBolt(),paralellism).shuffleGrouping("bolt_aggregate").setNumTasks(10);

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(12);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
