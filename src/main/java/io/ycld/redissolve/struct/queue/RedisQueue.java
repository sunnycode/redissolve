package io.ycld.redissolve.struct.queue;

import static io.ycld.redissolve.struct.queue.QueueUtil.MULTI_OPERATION_TIMEOUT_MS;
import static io.ycld.redissolve.struct.queue.QueueUtil.OPERATION_TIMEOUT_MS;
import static io.ycld.redissolve.struct.queue.QueueUtil.doBatchEnqueue;
import static io.ycld.redissolve.struct.queue.QueueUtil.doSingleEnqueue;
import static io.ycld.redissolve.struct.queue.QueueUtil.getAckKey;
import static io.ycld.redissolve.struct.queue.QueueUtil.getAckMap;
import static io.ycld.redissolve.struct.queue.QueueUtil.getMultiAcknowledge;
import static io.ycld.redissolve.struct.queue.QueueUtil.getQueueKey;
import static io.ycld.redissolve.struct.queue.QueueUtil.getSingleAcknowledge;
import static io.ycld.redissolve.struct.queue.QueueUtil.getSingleSize;
import static io.ycld.redissolve.struct.queue.QueueUtil.getUuidKey;
import io.ycld.redissolve.MultiOps;
import io.ycld.redissolve.RedisConfig;
import io.ycld.redissolve.RedisTemplates;
import io.ycld.redissolve.RedisTemplates.JedisCallback;
import io.ycld.redissolve.misc.Pair;
import io.ycld.redissolve.queue.ClusterConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RedisQueue {
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
}
