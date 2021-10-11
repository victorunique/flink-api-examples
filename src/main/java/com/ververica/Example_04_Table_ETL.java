package com.ververica;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.TimePointUnit;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.currentDate;
import static org.apache.flink.table.api.Expressions.jsonObject;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.table.api.Expressions.timestampDiff;

public class Example_04_Table_ETL {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    env.fromValues(
            row(12L, "Alice", LocalDate.of(1984, 3, 12)),
            row(32L, "Bob", LocalDate.of(1990, 10, 14)),
            row(7L, "Kyle", LocalDate.of(1979, 2, 23)))
        .as("c_id", "c_name", "c_birthday")
        .select(
            jsonObject(
                JsonOnNull.NULL,
                "name",
                $("c_name"),
                "birth_year",
                timestampDiff(TimePointUnit.YEAR, $("c_birthday"), currentDate())))
        .execute()
        .print();
  }
}
