package com.flink.demo;

import lombok.*;

import java.sql.Timestamp;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class User {

    private String userid;
    private String regionid;
    private String gender;
    private Timestamp registertime;



}
