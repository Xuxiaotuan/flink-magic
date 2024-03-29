package cn.xuyinyin.flink.magic.demo.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : XuJiaWei
 * @since : 2023-08-05 14:27
 */
public class MySqlSourceExample {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("100.106.104.56")
                .port(32278)
                // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .databaseList(".*")
                // 设置
                .tableList("xxt.datetime_table")
                .username("root")
                .password("asd123456")
                .scanNewlyAddedTableEnabled(true)
                // 将 SourceRecord 转换为 JSON 字符串
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4)
                // sink为直接输出
                .print()
                // 设置 sink 节点并行度为 1
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}