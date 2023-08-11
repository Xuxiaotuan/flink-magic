package cn.xuyinyin.flink.magic.demo.tidb;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.tikv.common.TiConfiguration;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;

/**
 * @author : XuJiaWei
 * @since : 2023-08-07 11:44
 */


public class TiDBSourceExample {

    public static void main(String[] args) throws Exception {


        TiConfiguration tiConfiguration = TDBSourceOptions.getTiConfiguration(
                "192.168.11.62:2379", new HashMap<>());
        String database = "test";
        String tableName = "test";
//        String tableName = "test,test1";

        SourceFunction<String> tidbSource =
                TiDBSource.<String>builder()
                        .database(database) // set captured database
                        .tableName(tableName) // set captured table
                        .tiConf(tiConfiguration)
                        .snapshotEventDeserializer(
                                new TiKVSnapshotEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Kvrpcpb.KvPair record, Collector<String> out)
                                            throws Exception {
                                        out.collect(record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .changeEventDeserializer(
                                new TiKVChangeEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Cdcpb.Event.Row record, Collector<String> out)
                                            throws Exception {
                                        out.collect(record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.addSource(tidbSource)
                .print()
                .setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }
}