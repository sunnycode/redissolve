package io.ycld.redissolve;

public class RedisConfig {
  private final String name;
  private final String host;
  private final int port;
  private final int db;

  public RedisConfig(final String name, final String host, final int port, final int db) {
    this.name = name;
    this.host = host;
    this.port = port;
    this.db = db;
  }

  public String getName() {
    return name;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getDb() {
    return db;
  }
}
