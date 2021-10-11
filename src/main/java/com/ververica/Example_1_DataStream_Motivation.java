package com.ververica;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example_1_DataStream_Motivation {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(ExampleData.CUSTOMERS)
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
