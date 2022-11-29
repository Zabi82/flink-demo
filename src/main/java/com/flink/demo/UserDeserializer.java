package com.flink.demo;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class UserDeserializer implements DeserializationSchema<User> {

  @Override
  public User deserialize(byte[] bytes) throws IOException {
    return Utils.getMapper().readValue(bytes, User.class);
  }

  @Override
  public boolean isEndOfStream(User user) {
    return false;
  }

  @Override
  public TypeInformation<User> getProducedType() {
    return TypeInformation.of(User.class);
  }
}
