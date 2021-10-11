package com.ververica;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.LocalDate;
import java.time.Period;

public class Example_3_DataStream_ETL {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(ExampleData.CUSTOMERS)
        .process(
            new ProcessFunction<Customer, String>() {
              @Override
              public void processElement(
                  Customer customer,
                  ProcessFunction<Customer, String>.Context context,
                  Collector<String> out)
                  throws JsonProcessingException {
                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode json = objectMapper.createObjectNode();
                json.put("name", customer.c_name);
                json.put(
                    "birth_year", Period.between(customer.c_birthday, LocalDate.now()).getYears());
                String output = objectMapper.writeValueAsString(json);
                out.collect(output);
              }
            })
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
