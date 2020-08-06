package com.aliyun.openservices.log.flink.sample;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.FastLogGroupDeserializer;
import com.aliyun.openservices.log.flink.data.FastLogGroupList;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class ConsumerSample {
    private static final String SLS_ENDPOINT = "cn-shanghai.log.aliyuncs.com";
    private static final String ACCESS_KEY_ID = "XXXXX";
    private static final String ACCESS_KEY_SECRET = "XXXXXX";
    private static final String SLS_PROJECT = "wzq-blink-test";
    private static final String SLS_LOGSTORE = "wzq-blink-test";
    //1、启动位点秒级的时间戳读取数据;2、读取全量加增量数据Consts.LOG_BEGIN_CURSOR;
    //3、读取增量数据Consts.LOG_END_CURSOR
    private static final String StartInMs = "1596424167";


    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // For local testing
        Configuration conf = new Configuration();
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                "file:///Users/kel/Github/flink3/aliyun-log-flink-connector/flink2");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend("file:///Users/kel/Github/flink3/aliyun-log-flink-connector/flink"));
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, SLS_ENDPOINT);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, ACCESS_KEY_ID);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, ACCESS_KEY_SECRET);
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "10");
        //
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, StartInMs);
//        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_FROM_CHECKPOINT);
        configProps.put(ConfigConstants.LOG_CONSUMERGROUP, "23_ots_sla_etl_product1");
        configProps.put(ConfigConstants.LOG_CHECKPOINT_MODE, CheckpointMode.ON_CHECKPOINTS.name());
        configProps.put(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS, "10000");
        FastLogGroupDeserializer deserializer = new FastLogGroupDeserializer();
        DataStream<FastLogGroupList> stream = env.addSource(
                new FlinkLogConsumer<>(SLS_PROJECT, SLS_LOGSTORE, deserializer, configProps));

        // stream.print();

        stream.flatMap(new FlatMapFunction<FastLogGroupList, String>() {
            @Override
            public void flatMap(FastLogGroupList fastLogGroupList, Collector<String> collector) throws Exception {

                for (FastLogGroup aaa : fastLogGroupList.getLogGroups()) {
                    int logCount = aaa.getLogsCount();
                    for (int i = 0; i < logCount; i++) {
                        FastLog logs = aaa.getLogs(i);
                        int logsCount = logs.getContentsCount();
//                      for (int a = 0; a < logsCount; a++) {
//                         FastLogContent log = logs.getContents(logsCount-1);
//
//                          System.out.print(log.getValue());
//                      }
                        FastLogContent log = logs.getContents(logsCount-1);
                        System.out.println(log.getValue());
                        // processing log
                    }
                }
            }
        });


//        stream.flatMap((FlatMapFunction<FastLogGroupList, String>) (value, out) -> {
//            for (FastLogGroup logGroup : value.getLogGroups()) {
//                int logCount = logGroup.getLogsCount();
//                for (int i = 0; i < logCount; i++) {
//                    FastLog log = logGroup.getLogs(i);
//
//                    // processing log
//                    System.out.print(log);
//                }
//            }
//        });

        stream.writeAsText("log-" + System.nanoTime());
        env.execute("sls_consumer");
    }
}
