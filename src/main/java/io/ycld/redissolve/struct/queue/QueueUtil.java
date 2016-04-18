package io.ycld.redissolve.struct.queue;

import io.ycld.redissolve.RedisTemplates;
import io.ycld.redissolve.RedisTemplates.JedisCallback;
import io.ycld.redissolve.misc.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class QueueUtil {
  public static final long OPERATION_TIMEOUT_MS = Long.parseLong(System.getProperty("op.timeout",
      "1000"));
  public static final long MULTI_OPERATION_TIMEOUT_MS = Long.parseLong(System.getProperty(
      "multiop.timeout", "4000"));

  public static final int PERSISTENCE_WINDOW_SECS = 4 * 60 * 60; // 4h as seconds
  public static final DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd'T'HHmm")
      .withZone(DateTimeZone.UTC);

  public static JedisCallback<Boolean> doSingleEnqueue(final String nodeId, final String queueName,
      final String uuid, final String entry, final boolean isFallback) {
    return new JedisCallback<Boolean>() {
      @Override
      public Boolean withJedis(Jedis jedis) {
        String uuidKey = getUuidKey(queueName, uuid);
        String queueKey = getQueueKey(nodeId, queueName);

        Transaction txn1 = jedis.multi();

        if (!isFallback) {
          txn1.set(uuidKey, entry);
          txn1.expire(uuidKey, PERSISTENCE_WINDOW_SECS);
        }

        txn1.rpush(queueKey, uuid);
        txn1.expire(queueKey, PERSISTENCE_WINDOW_SECS);

        txn1.exec();

        return true;
      }
    };
  }

  public static void doBatchEnqueue(final String nodeId, final JedisPool thePool,
      final String queueName, final DateTime timestamp, final Set<String> uuids) {
    RedisTemplates.withJedisCallback(thePool, new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        String queueKey = getQueueKey(nodeId, queueName);

        Transaction txn1 = jedis.multi();

        txn1.rpush(queueKey, uuids.toArray(new String[] {}));
        txn1.expire(queueKey, PERSISTENCE_WINDOW_SECS);

        txn1.exec();

        return null;
      }
    });
  }

  public static JedisCallback<Long> getSingleSize(final String queueKey) {
    return new JedisCallback<Long>() {
      @Override
      public Long withJedis(Jedis jedis) {
        long size = jedis.llen(queueKey);

        return size;
      }
    };
  }

  public static JedisCallback<Void> getSingleAcknowledge(final String ackKey, final String uuid) {
    return new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        Transaction txn1 = jedis.multi();

        txn1.sadd(ackKey, uuid);
        txn1.expire(ackKey, PERSISTENCE_WINDOW_SECS);

        txn1.exec();

        return null;
      }
    };
  }

  public static JedisCallback<Void> getMultiAcknowledge(final Map<String, List<String>> entries) {
    return new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        Transaction txn1 = jedis.multi();

        for (Map.Entry<String, List<String>> entry : entries.entrySet()) {
          String ackKey = entry.getKey();

          for (String uuid : entry.getValue()) {
            txn1.sadd(ackKey, uuid);
          }

          txn1.expire(ackKey, PERSISTENCE_WINDOW_SECS);
        }

        txn1.exec();

        return null;
      }
    };
  }

  public static Map<String, List<String>> getAckMap(String theNodeId, String theQueue,
      List<Pair<DateTime, String>> entries) {
    final Map<String, List<String>> ackMap = new LinkedHashMap<String, List<String>>();

    for (Pair<DateTime, String> entry : entries) {
      String ackKey = getAckKey(theNodeId, theQueue, entry.first);

      if (!ackMap.containsKey(ackKey)) {
        ackMap.put(ackKey, new ArrayList<String>());
      }

      ackMap.get(ackKey).add(entry.second);
    }

    return ackMap;
  }

  public static String getUuidKey(final String queueName, final String uuid) {
    return ("e:" + queueName + ":" + uuid);
  }

  public static String getQueueKey(final String nodeId, final String queueName) {
    return ("q:" + queueName + ":" + nodeId);
  }

  public static String getAckKey(final String nodeId, final String queueName,
      final DateTime timestamp) {
    return ("a:" + queueName + ":" + nodeId + ":" + format.print(timestamp));
  }
}
