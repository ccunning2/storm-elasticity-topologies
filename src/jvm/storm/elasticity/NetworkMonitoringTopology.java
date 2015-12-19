package storm.elasticity;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.elasticity.spout.RandomSentenceSpout;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


/**
 * Created by jerrypeng on 12/19/15.
 */
public class NetworkMonitoringTopology {

    public class NetworkMonitoringTopologySpout extends BaseRichSpout {
        SpoutOutputCollector _collector;

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
        }

        public void nextTuple() {
            _collector.emit(new Values(StringGenerator()));
        }

        public String StringGenerator() {
            UUID id = UUID.randomUUID();
            long unixTime = System.currentTimeMillis() / 1000L;
            String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
                    "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature", "Jerry had good day",
                    "Hello", "May the force be with you", "Computer Science"};
            Random rand = new Random();
            String sentence = sentences[rand.nextInt(sentences.length)];
            String[] tags = new String[]{"Success", "Fail"};
            String tag = tags[rand.nextInt(tags.length)];
            String ret = "{\"id\": \"" + id + "\" ,\"time\": " + unixTime + ", \"sentence\": \"" + sentence + "\", \"tag\": \"" + tag + "\"}";

            return ret;
        }
    }

    public static class ParseLines implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            JSONObject obj = new JSONObject(tuple.getString(0));


            _collector.emit(tuple, new Values(obj.getString("id"),
                    obj.getString("time"),
                    obj.getString("sentence"),
                    obj.getString("tag")));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id", "time", "sentence", "tag"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class FilterSuccess implements IRichBolt {

        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            if (tuple.getStringByField("tag").equals("Success")) {
                _collector.emit(tuple.getValues());
            }
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id", "time", "sentence", "tag"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    public static class FilterFailure implements IRichBolt {

        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            if (tuple.getStringByField("tag").equals("Fail")) {
                _collector.emit(tuple.getValues());
            }
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id", "time", "sentence", "tag"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class ParseFailures implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] split = sentence.split(" ");
            String parseResult = "";
            if(split.length > 4) {
                parseResult = "large message failure";
            } else {
                parseResult = "small message failure";
            }

            _collector.emit(tuple, new Values(tuple.getStringByField("id"),
                    tuple.getStringByField("time"), parseResult, tuple.getStringByField("tag")));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id", "time", "sentence", "tag"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class ParseSuccess implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] split = sentence.split(" ");
            String parseResult = "";
            if(split.length > 4) {
                parseResult = "large success message";
            } else {
                parseResult = "small success message";
            }

            _collector.emit(tuple, new Values(parseResult, 1));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence", "count"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Aggregate implements IRichBolt {
        OutputCollector _collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            if(!this.counts.containsKey(sentence)) {
                this.counts.put(sentence, 0);
            }
            int existingCount = this.counts.get(sentence);
            this.counts.put(sentence, existingCount + 1);

            _collector.emit(tuple, new Values(sentence, this.counts.get(sentence)));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence", "count"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Filter implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            if(tuple.getIntegerByField("count") > 5) {
                _collector.emit(tuple, tuple.getValues());
            }
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence", "count"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Functor implements IRichBolt {
        OutputCollector _collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("sentence");
            Integer length=word.length();
            word=word.substring(0,(int)(0.8*word.length()));
            _collector.emit(tuple, new Values(word, tuple.getValueByField("count")));
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence", "count"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Join implements IRichBolt {
        OutputCollector _collector;
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            int count = tuple.getIntegerByField("count");
            if(!this.counts.containsKey(count)) {
                this.counts.put(count, 0);
            }

            int existingValue = this.counts.get(count);
            this.counts.put(count, existingValue + 1);
        }

        public void cleanup() {

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 5);
        builder.setBolt("parseLines", new ParseLines(), 5).shuffleGrouping("spout");
        builder.setBolt("filterFailure", new FilterFailure(), 5).shuffleGrouping("parseLines");
        builder.setBolt("parseFailures", new ParseFailures(), 5).shuffleGrouping("filterFailure");
        //change id
        builder.setBolt("aggregate", new Aggregate(), 5).fieldsGrouping("parseFailures", new Fields("id"));
        builder.setBolt("filter", new Functor(), 5).shuffleGrouping("aggregate");
        builder.setBolt("functor", new Functor(), 5).shuffleGrouping("filter");


        builder.setBolt("filterSuccess", new FilterSuccess(), 5).shuffleGrouping("parseLines");
        builder.setBolt("parseSuccess", new ParseSuccess(), 5).shuffleGrouping("filterSuccess");

        builder.setBolt("join", new Join(), 5)
                .fieldsGrouping("parseSuccess", new Fields("count"))
                .fieldsGrouping("functor", new Fields("count"));

        Config conf = new Config();
        conf.setDebug(true);
    }
}
