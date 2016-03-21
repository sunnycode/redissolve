package io.ycld.redissolve.struct.map;

import io.ycld.redissolve.MultiOps;
import io.ycld.redissolve.RedisConfig;
import io.ycld.redissolve.RedisTemplates;
import io.ycld.redissolve.RedisTemplates.JedisCallback;
import io.ycld.redissolve.misc.JsonUtils;
import io.ycld.redissolve.queue.ClusterConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RedisMap {
  private final Map<String, JedisPool> NODES;
  private final List<JedisPool> POOLS;

  private final String nodeId;

  @Inject
  public RedisMap(ClusterConfig config) {
    ImmutableMap.Builder<String, JedisPool> theNodes = ImmutableMap.builder();

    for (Map.Entry<String, RedisConfig> node : config.CLUSTER_NODES.entrySet()) {
      String nodeId = node.getKey();
      RedisConfig redisConfig = node.getValue();

      JedisPoolConfig cfg = new JedisPoolConfig();
      cfg.setTestOnCreate(true);
      cfg.setTestOnBorrow(true);
      cfg.setTestOnReturn(false);
      cfg.setTestWhileIdle(true);
      cfg.setMaxIdle(4000);
      cfg.setMaxTotal(4000);
      cfg.setMaxWaitMillis(10000);

      JedisPool jedis =
          new JedisPool(cfg, redisConfig.getHost(), redisConfig.getPort(), 10000, null,
              redisConfig.getDb());

      theNodes.put(nodeId, jedis);
    }

    this.nodeId = config.THIS_NODE;
    this.NODES = theNodes.build();
    this.POOLS = ImmutableList.<JedisPool>builder().addAll(this.NODES.values()).build();
  }

  public void pingLocal() {
    final JedisPool thisPool = this.NODES.get(this.nodeId);

    RedisTemplates.withJedisCallback(thisPool, new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        String result = jedis.ping();

        if (!"PONG".equals(result)) {
          throw new RuntimeException("invalid Redis PING response! " + result);
        }

        return null;
      }
    });
  }

  public void setAll(final byte[] key, final byte[] value, final int ttl) {
    ImmutableList.Builder<JedisCallback<Void>> setCallbacks = ImmutableList.builder();

    for (int i = 0; i < this.NODES.size(); i++) {
      setCallbacks.add(createSetCallback(key, value, ttl));
    }

    MultiOps.doParallel(this.POOLS, setCallbacks.build(), null, 100);
  }

  public List<byte[]> getAll(final byte[] key) {
    ImmutableList.Builder<JedisCallback<byte[]>> getCallbacks = ImmutableList.builder();

    for (int i = 0; i < this.NODES.size(); i++) {
      getCallbacks.add(createGetCallback(key));
    }

    final List<byte[]> result = MultiOps.doParallel(this.POOLS, getCallbacks.build(), null, 100);

    return Collections.unmodifiableList(result);
  }

  private JedisCallback<byte[]> createGetCallback(final byte[] key) {
    return new JedisCallback<byte[]>() {
      @Override
      public byte[] withJedis(Jedis jedis) {
        return jedis.get(key);
      }
    };
  }

  private JedisCallback<Void> createSetCallback(final byte[] key, final byte[] value, final int ttl) {
    return new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        Transaction txn1 = jedis.multi();

        txn1.set(key, value);
        txn1.expire(key, ttl);

        txn1.exec();

        return null;
      }
    };
  }

  public byte[] getLatest(final byte[] key) {
    List<byte[]> results = getAll(key);
    byte[] latest = null;
    String latestTs = "";

    for (byte[] entry : results) {
      if (entry == null) {
        continue;
      }

      if (latest == null && entry != null) {
        latest = entry;
      }

      LinkedHashMap<String, Object> jsonVal = JsonUtils.fromJson(LinkedHashMap.class, entry);

      if (!jsonVal.containsKey("ts")) {
        continue;
      }

      String ts = (String) jsonVal.get("ts");

      if (latestTs.compareTo(ts) < 0) {
        latestTs = ts;
        latest = entry;
      }
    }

    return latest;
  }
}
