/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * An example topology that demonstrates the use of {@link org.apache.storm.topology.IStatefulBolt}
 * to manage state. To run the example,
 * <pre>
 * $ storm jar examples/storm-starter/storm-starter-topologies-*.jar storm.starter.StatefulTopology statetopology
 * </pre>
 * <p/>
 * The default state used is 'InMemoryKeyValueState' which does not persist the state across restarts. You could use
 * 'RedisKeyValueState' to test state persistence by setting below property in conf/storm.yaml
 * <pre>
 * topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider
 * </pre>
 * <p/>
 * You should also start a local redis instance before running the 'storm jar' command. The default
 * RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
 * <p/>
 * <pre>
 * topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
 *                                   "keySerializerClass":"...", "valueSerializerClass":"...",
 *                                   "jedisPoolConfig":{"host":"localhost", "port":6379,
 *                                      "timeout":2000, "database":0, "password":"xyz"}}'
 *
 * </pre>
 * </p>
 */
public class StatefulTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
    /**
     * A bolt that uses {@link KeyValueState} to save its state.
     */
    private static class StatefulSumBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
        String name;
        KeyValueState<String, Long> kvState;
        long sum;
        private OutputCollector collector;

        StatefulSumBolt(String name) {
            this.name = name;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            sum += ((Number) input.getValueByField("value")).longValue();
            LOG.debug("{} sum = {}", name, sum);
            kvState.put("sum", sum);
            collector.emit(input, new Values(sum));
        }

        @Override
        public void initState(KeyValueState<String, Long> state) {
            kvState = state;
            sum = kvState.get("sum", 0L);
            LOG.debug("Initstate, sum from saved state = {} ", sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple);
            LOG.debug("Got tuple {}", tuple);
            collector.emit(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("value"));
        }

    }

    public static void main(String[] args) throws Exception {

//        final CommonMongoClient commonMongoClient = new CommonMongoClient();
//        commonMongoClient.open();
//        Document entry = new Document();
//        entry.append("component", "TopologyMain");
//        entry.append("timestamp", new Date());
//        commonMongoClient.insertDocumentToDB("statefulTopology.tuples", entry);
        int memKoef=4;
        int cpuKoef=12;
        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer spout = builder.setSpout("spout", new RandomIntegerSpout(){ });
        spout.setMemoryLoad(64*memKoef);
        spout.setCPULoad(15*cpuKoef);
        BoltDeclarer bolt = builder.setBolt("partialsum", new StatefulSumBolt("partial"), 2).directGrouping("spout");
        bolt.setMemoryLoad(128*memKoef);
        bolt.setCPULoad(10*cpuKoef);
        bolt = builder.setBolt("printer", new PrinterBolt(), 1).allGrouping("partialsum");
        bolt.setMemoryLoad(74*memKoef);
        bolt.setCPULoad(15*cpuKoef);
        //bolt = builder.setBolt("total", new StatefulSumBolt("total"), 1).shuffleGrouping("printer");
        //bolt = builder.setBolt("total", new StatefulSumBolt("total"), ).shuffleGrouping("printer");

//        bolt = builder.setBolt("total2", new StatefulSumBolt("total2"), 1).shuffleGrouping("total");
        spout.setMemoryLoad(128*memKoef);
        spout.setCPULoad(10*cpuKoef);
        Config conf = new Config();
        conf.setDebug(false);

        conf.setNumWorkers(1);
        //StormSubmitter.submitTopologyWithProgressBar(null, conf, builder.createTopology());

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            //Map clusterConf = Utils.readStormConfig();
            //StormSubmitter.submitTopologyWithProgressBar("StatefulTopology", conf, builder.createTopology());
            //StormSubmitter.submitTopologyWithProgressBar("test1", conf, builder.createTopology());
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            for(int i=0; i<1;i++){
                cluster.submitTopology("test"+i, conf, topology);
            }
            Utils.sleep(5*60*1000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
