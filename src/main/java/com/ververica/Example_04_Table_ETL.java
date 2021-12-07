package com.ververica;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.row;

/** Use built-in functions to perform streaming ETL i.e. convert records into JSON. */
public class Example_04_Table_ETL {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    env.fromValues(
            row(12L, "Alice", LocalDate.of(1984, 3, 12)),
            row(32L, "Bob", LocalDate.of(1990, 10, 14)),
            row(7L, "Kyle", LocalDate.of(1979, 2, 23)))
        .as("c_id", "c_name", "c_birthday")
        // only available in Flink 1.15
        //        .select(
        //            jsonObject(
        //                JsonOnNull.NULL,
        //                "name",
        //                $("c_name"),
        //                "age",
        //                timestampDiff(TimePointUnit.YEAR, $("c_birthday"), currentDate())))
        .execute()
        .print();
  }
}
