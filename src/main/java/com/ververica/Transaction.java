package com.ververica;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.Instant;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Transaction {
  public long t_id;
  public Instant t_time;
  public long t_customer_id;
  public BigDecimal t_amount;
}
