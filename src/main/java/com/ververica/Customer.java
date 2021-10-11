package com.ververica;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Customer {
    public long c_id;
    public String c_name;
    public LocalDate c_birthday;
}
