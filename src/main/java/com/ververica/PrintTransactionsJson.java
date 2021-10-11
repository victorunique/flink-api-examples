package com.ververica;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.ververica.Utils.getMapper;

public class PrintTransactionsJson {

  // bin/zookeeper-server-start.sh config/zookeeper.properties &

  // bin/kafka-server-start.sh config/server.properties &

  // bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
  // --partitions=1 --replication-factor=1 &

  // bin/kafka-console-producer.sh --topic transactions --bootstrap-server localhost:9092

  // bin/kafka-console-consumer.sh --topic transactions --from-beginning --bootstrap-server
  // localhost:9092

  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper mapper = getMapper();
    for (Transaction transaction : ExampleData.TRANSACTIONS) {
      String s = mapper.writeValueAsString(transaction);
      System.out.println(s);
    }
  }
}
