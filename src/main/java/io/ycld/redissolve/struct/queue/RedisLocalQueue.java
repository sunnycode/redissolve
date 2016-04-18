package io.ycld.redissolve.struct.queue;

import static io.ycld.redissolve.struct.queue.QueueUtil.doSingleEnqueue;
import static io.ycld.redissolve.struct.queue.QueueUtil.getQueueKey;
import static io.ycld.redissolve.struct.queue.QueueUtil.getSingleSize;
import static io.ycld.redissolve.struct.queue.QueueUtil.getUuidKey;
import io.ycld.redissolve.RedisConfig;
import io.ycld.redissolve.RedisTemplates;
import io.ycld.redissolve.RedisTemplates.JedisCallback;
import io.ycld.redissolve.misc.Pair;

import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisLocalQueue {
  private final JedisPool NODE;
  private final JedisPool POOL;

  private final String nodeId;

  private final String queueName;

  @Inject
  public RedisLocalQueue(RedisConfig config) {
    String queueName = "theQ";

    JedisPoolConfig cfg = new JedisPoolConfig();
    cfg.setTestOnCreate(true);
    cfg.setTestOnBorrow(true);
    cfg.setTestOnReturn(false);
    cfg.setTestWhileIdle(true);
    cfg.setMaxIdle(4000);
    cfg.setMaxTotal(4000);
    cfg.setMaxWaitMillis(10000);

    JedisPool jedis =
        new JedisPool(cfg, config.getHost(), config.getPort(), 10000, null, config.getDb());

    this.nodeId = config.getName();

    this.NODE = jedis;
    this.POOL = jedis;

    this.queueName = queueName;
  }

  public void pingLocal() {
    RedisTemplates.withJedisCallback(this.NODE, new JedisCallback<Void>() {
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
    RedisTemplates.withJedisCallback(this.NODE,
        doSingleEnqueue(this.nodeId, queueName, uuid, entry, false));
  }

  public void drainTo(List<String> toList, int count) {
    doDrainTo(this.nodeId, this.NODE, nodeId, queueName, toList, count - toList.size());
  }

  public void acknowledge(DateTime timestamp, String uuid) {
    throw new UnsupportedOperationException();
  }

  public void acknowledgeMulti(List<Pair<DateTime, String>> entries) {
    throw new UnsupportedOperationException();
  }

  public long size() {
    return RedisTemplates.withJedisCallback(this.POOL,
        getSingleSize(getQueueKey(this.nodeId, queueName)));
  }

  public boolean isEmpty() {
    return this.size() < 1;
  }

  public int performAntiEntropy(final DateTime timestamp) {
    throw new UnsupportedOperationException();
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
