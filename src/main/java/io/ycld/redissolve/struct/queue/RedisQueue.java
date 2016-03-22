package io.ycld.redissolve.struct.queue;

import io.ycld.redissolve.MultiOps;
import io.ycld.redissolve.RedisConfig;
import io.ycld.redissolve.RedisTemplates;
import io.ycld.redissolve.RedisTemplates.JedisCallback;
import io.ycld.redissolve.misc.Pair;
import io.ycld.redissolve.queue.ClusterConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RedisQueue {
  private static final long OPERATION_TIMEOUT_MS = 250;
  private static final long MULTI_OPERATION_TIMEOUT_MS = 500;

  private static final int PERSISTENCE_WINDOW_SECS = 4 * 60 * 60; // 4h as seconds
  private static final DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd'T'HHmm")
      .withZone(DateTimeZone.UTC);

  private final Map<String, JedisPool> NODES;
  private final List<JedisPool> POOLS;

  private final String nodeId;

  private final String queueName;

  @Inject
  public RedisQueue(ClusterConfig config) {
    String queueName = "theQ";

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

    this.queueName = queueName;
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

  public void enqueue(DateTime timestamp, String uuid, String entry) {
    ImmutableList.Builder<JedisCallback<Boolean>> enqueueCallbacks = ImmutableList.builder();

    for (String otherNodeId : this.NODES.keySet()) {
      enqueueCallbacks.add(doSingleEnqueue(otherNodeId, queueName, uuid, entry, false));
    }

    // perform enqueue in parallel
    List<Boolean> results =
        MultiOps.doParallel(this.POOLS, enqueueCallbacks.build(), false, OPERATION_TIMEOUT_MS);

    // enqueue any errors locally using non-parallel path
    JedisPool thisPool = this.NODES.get(this.nodeId);

    int i = 0;
    for (String otherNodeId : this.NODES.keySet()) {
      boolean remoteResult = results.get(i);

      if (!remoteResult) {
        // fallback = true if was remote
        boolean wasRemote = otherNodeId.equals(this.nodeId);

        System.out.println(new DateTime() + " ENQUEUE_RETRY " + uuid + " @ " + otherNodeId + " ("
            + (wasRemote ? "remote" : "local") + ")");

        RedisTemplates.withJedisCallback(thisPool,
            doSingleEnqueue(otherNodeId, queueName, uuid, entry, wasRemote));
      }

      i += 1;
    }
  }

  public void drainTo(List<String> toList, int count) {
    int initialSize = toList.size();
    int maxSize = initialSize + count;

    // randomize queue checks
    List<String> toCheck = new ArrayList<String>(this.NODES.size());
    toCheck.addAll(this.NODES.keySet());
    Collections.shuffle(toCheck);

    // check other nodes for my entries
    for (String theNode : toCheck) {
      JedisPool pool = this.NODES.get(theNode);

      doDrainTo(theNode, pool, nodeId, queueName, toList, maxSize - toList.size());

      if (toList.size() >= maxSize) {
        break;
      }
    }
  }

  public void acknowledge(DateTime timestamp, String uuid) {
    ImmutableList.Builder<JedisCallback<Void>> ackCallbacks =
        ImmutableList.<JedisCallback<Void>>builder();

    for (int i = 0; i < this.POOLS.size(); i++) {
      ackCallbacks
          .add(getSingleAcknowledge(getAckKey(this.nodeId, this.queueName, timestamp), uuid));
    }

    MultiOps.<Void>doParallel(this.POOLS, ackCallbacks.build(), null, OPERATION_TIMEOUT_MS);
  }

  public void acknowledgeMulti(List<Pair<DateTime, String>> entries) {
    ImmutableList.Builder<JedisCallback<Void>> ackCallbacks =
        ImmutableList.<JedisCallback<Void>>builder();

    Map<String, List<String>> entriesByAckKey = getAckMap(this.nodeId, this.queueName, entries);

    for (int i = 0; i < this.POOLS.size(); i++) {
      ackCallbacks.add(getMultiAcknowledge(entriesByAckKey));
    }

    MultiOps.<Void>doParallel(this.POOLS, ackCallbacks.build(), null, MULTI_OPERATION_TIMEOUT_MS);
  }

  public long size() {
    ImmutableList.Builder<JedisCallback<Long>> sizeCallbacks =
        ImmutableList.<JedisCallback<Long>>builder();

    for (int i = 0; i < this.POOLS.size(); i++) {
      sizeCallbacks.add(getSingleSize(getQueueKey(this.nodeId, queueName)));
    }

    List<Long> sizes =
        MultiOps.doParallel(this.POOLS, sizeCallbacks.build(), 0L, OPERATION_TIMEOUT_MS);

    long result = 0L;

    for (Long size : sizes) {
      if (size != null) {
        result += size.longValue();
      }
    }

    return result;
  }

  public boolean isEmpty() {
    return this.size() < 1;
  }

  public int performAntiEntropy(final DateTime timestamp) {
    final JedisPool thisPool = this.NODES.get(this.nodeId);

    int found = 0;

    for (Map.Entry<String, JedisPool> node : this.NODES.entrySet()) {
      final String otherNodeId = node.getKey();

      if (otherNodeId.equals(this.nodeId)) {
        continue;
      }

      found += doAntiEntropy(this.nodeId, otherNodeId, thisPool, this.queueName, timestamp);
    }

    return found;
  }

  private static int doAntiEntropy(final String sourceNode, final String targetNode,
      final JedisPool thePool, final String queueName, final DateTime timestamp) {

    Set<String> found = RedisTemplates.withJedisCallback(thePool, new JedisCallback<Set<String>>() {
      @Override
      public Set<String> withJedis(Jedis jedis) {
        String sourceAckKey = getAckKey(sourceNode, queueName, timestamp);
        String targetAckKey = getAckKey(targetNode, queueName, timestamp);

        return jedis.sdiff(sourceAckKey, targetAckKey);
      }
    });

    if (found.size() > 0) {
      System.out.println("anti-entropy found " + found.size() + " entries in " + queueName + " at "
          + sourceNode + " for " + targetNode + " -> " + found);

      doBatchEnqueue(targetNode, thePool, queueName, timestamp, found);
    }

    return found.size();
  }

  private static JedisCallback<Boolean> doSingleEnqueue(final String nodeId,
      final String queueName, final String uuid, final String entry, final boolean isFallback) {
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

  private static void doBatchEnqueue(final String nodeId, final JedisPool thePool,
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

  private static JedisCallback<Long> getSingleSize(final String queueKey) {
    return new JedisCallback<Long>() {
      @Override
      public Long withJedis(Jedis jedis) {
        long size = jedis.llen(queueKey);

        return size;
      }
    };
  }

  public void doDrainTo(final String theNode, final JedisPool pool, final String nodeId,
      final String queueName, final List<String> toList, final int count) {
    final String queueKey = getQueueKey(nodeId, queueName);

    RedisTemplates.withJedisCallback(pool, new JedisCallback<Void>() {
      @Override
      public Void withJedis(Jedis jedis) {
        Transaction txn1 = jedis.multi();
        Response<List<String>> entries = txn1.lrange(queueKey, 0, count - 1);
        txn1.ltrim(queueKey, count, -1);
        txn1.exec();

        List<String> todo = entries.get();

        if (todo.size() < 1) {
          // System.out.println("got 0 entries in " + queueName + " for " + nodeId + " at " +
          // theNode);
          return null;
        }

        String[] todoKeys = new String[todo.size()];

        for (int i = 0; i < todo.size(); i++) {
          String uuidBytes = todo.get(i);
          String uuidKey = getUuidKey(queueName, uuidBytes);
          todoKeys[i] = uuidKey;
        }

        List<String> foundEntries = jedis.mget(todoKeys);
        toList.addAll(foundEntries);

        // System.out.println("got " + foundEntries.size() + " entries in " + queueName + " for "
        // + nodeId + " at " + theNode);

        return null;
      }
    });
  }

  private static JedisCallback<Void> getSingleAcknowledge(final String ackKey, final String uuid) {
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

  private static JedisCallback<Void> getMultiAcknowledge(final Map<String, List<String>> entries) {
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

  private static Map<String, List<String>> getAckMap(String theNodeId, String theQueue,
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

  private static String getUuidKey(final String queueName, final String uuid) {
    return ("e:" + queueName + ":" + uuid);
  }

  private static String getQueueKey(final String nodeId, final String queueName) {
    return ("q:" + queueName + ":" + nodeId);
  }

  private static String getAckKey(final String nodeId, final String queueName,
      final DateTime timestamp) {
    return ("a:" + queueName + ":" + nodeId + ":" + format.print(timestamp));
  }
}
