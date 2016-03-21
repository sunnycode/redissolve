package io.ycld.redissolve.queue;

import io.ycld.redissolve.RedisConfig;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class ClusterConfig {
  public Map<String, RedisConfig> CLUSTER_NODES = ImmutableMap.of(
      //
      System.getProperty("node1", "q01"),
      new RedisConfig(System.getProperty("node1.ip", "127.0.0.1"), Integer.parseInt(System
          .getProperty("node1.port", "6379")),
          Integer.parseInt(System.getProperty("node1.db", "1"))),
      //
      System.getProperty("node2", "q02"),
      new RedisConfig(System.getProperty("node2.ip", "127.0.0.1"), Integer.parseInt(System
          .getProperty("node2.port", "6479")),
          Integer.parseInt(System.getProperty("node2.db", "1"))),
      //
      System.getProperty("node3", "q03"),
      new RedisConfig(System.getProperty("node3.ip", "127.0.0.1"), Integer.parseInt(System
          .getProperty("node3.port", "6579")),
          Integer.parseInt(System.getProperty("node3.db", "1"))));

  public String THIS_NODE = System.getProperty("this.node", "q01");
}
