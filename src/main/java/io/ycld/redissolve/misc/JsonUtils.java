package io.ycld.redissolve.misc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;

public class JsonUtils {
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  private static final SmileFactory smile = new SmileFactory();
  private static final ObjectMapper smileMapper = new ObjectMapper(smile);

  static {
    jsonMapper.registerModule(new JodaModule());
    smileMapper.registerModule(new JodaModule());
  }

  public static String asJson(Object obj) {
    try {
      return jsonMapper.writeValueAsString(obj);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromJson(Class<T> clazz, String value) {
    try {
      return jsonMapper.readValue(value, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromJson(Class<T> clazz, byte[] json) {
    try {
      return jsonMapper.readValue(json, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromSmile(Class<T> clazz, byte[] bytes) {
    try {
      return smileMapper.readValue(bytes, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static byte[] asSmile(Object instance) {
    try {
      return smileMapper.writeValueAsBytes(instance);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
