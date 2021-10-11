package com.ververica;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example_7_Table_Deduplicate_Join {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Customer> customerStream = env.fromElements(ExampleData.CUSTOMERS);
    tableEnv.createTemporaryView("Customers", customerStream);

    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            // .setBounded(OffsetsInitializer.latest())
            .build();
    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");
    tableEnv.createTemporaryView("Transactions", transactionStream);

    tableEnv
        .executeSql(
            "SELECT c_name, CAST(t_amount AS DECIMAL(5, 2))\n"
                + "FROM Customers JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id")
        .print();
  }
}
