package com.flink.demo;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class UserSerializer implements SerializationSchema<User> {


  @Override
  public byte[] serialize(User user) {
    byte[] byteArray = null;
    try {
       byteArray = Utils.getMapper().writeValueAsBytes(user);
    } catch (Exception e) {
      throw new RuntimeException("Exception serializing user object " + user);
    }
    return byteArray;


  }
}
