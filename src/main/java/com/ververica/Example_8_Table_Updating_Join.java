package com.ververica;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

public class Example_8_Table_Updating_Join {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Row> customerStream =
        env.fromElements(ExampleData.CUSTOMERS_WITH_UPDATES)
            .returns(
                Types.ROW_NAMED(
                    new String[] {"c_id", "c_name", "c_birthday"},
                    Types.LONG,
                    Types.STRING,
                    Types.LOCAL_DATE));

    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .build();

    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

    tableEnv.createTemporaryView(
        "Customers",
        tableEnv.fromChangelogStream(
            customerStream,
            Schema.newBuilder().primaryKey("c_id").build(),
            ChangelogMode.upsert()));
    tableEnv.createTemporaryView("Transactions", transactionStream);

    tableEnv
        .executeSql(
            "SELECT c_name, CAST(t_amount AS DECIMAL(5, 2))\n"
                + "FROM Customers LEFT JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id")
        .print();
  }
}
