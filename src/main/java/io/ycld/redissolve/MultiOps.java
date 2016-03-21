package io.ycld.redissolve;

import io.ycld.redissolve.RedisTemplates.JedisCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.JedisPool;

import com.google.common.base.Throwables;

public class MultiOps {
  private static long MIN_DELAY_PARALLEL = 5L;

  public static <T> List<T> doParallel(final List<JedisPool> pools,
      final List<JedisCallback<T>> callbacks, final T defaultValue, final long timeoutMs) {
    ExecutorService executor = Executors.newFixedThreadPool(pools.size());

    List<Future<T>> futures = new ArrayList<Future<T>>();

    for (int i = 0; i < pools.size(); i++) {
      JedisPool pool = pools.get(i);
      JedisCallback<T> callback = callbacks.get(i);

      futures.add(executor.submit(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return RedisTemplates.withJedisCallback(pool, callback);
        }
      }));
    }

    long timeoutRemaining = timeoutMs;
    final long t1 = System.currentTimeMillis();

    List<T> results = new ArrayList<T>();

    for (Future<T> future : futures) {
      try {
        T value = future.get(timeoutRemaining, TimeUnit.MILLISECONDS);
        results.add(value);

        long t2 = System.currentTimeMillis();
        timeoutRemaining = Math.max(timeoutMs - (t2 - t1), MIN_DELAY_PARALLEL);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw Throwables.propagate(e);
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw Throwables.propagate(e);
      } catch (TimeoutException e) {
        e.printStackTrace();
        results.add(defaultValue);
      }
    }

    executor.shutdownNow();

    return results;
  }

  public static <T> List<T> doSerial(List<JedisPool> pools, List<JedisCallback<T>> callbacks,
      T defaultValue, long timeoutMs) {
    List<T> results = new ArrayList<T>();

    for (int i = 0; i < pools.size(); i++) {
      JedisPool pool = pools.get(i);
      JedisCallback<T> callback = callbacks.get(i);

      try {
        results.add(RedisTemplates.withJedisCallback(pool, callback));
      } catch (Exception e) {
        e.printStackTrace();
        results.add(defaultValue);
      }
    }

    return Collections.unmodifiableList(results);
  }
}
