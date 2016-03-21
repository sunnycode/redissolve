package io.ycld.redissolve;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisTemplates {
  public static <T> T withJedisCallback(JedisPool jedisPool, JedisCallback<T> jedisCallback) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedisCallback.withJedis(jedis);
    }
  }

  public interface JedisCallback<T> {
    public T withJedis(Jedis jedis);
  }
}
