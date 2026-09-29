package org.redisson;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RedissonLockExpirationRenewalTest extends RedisDockerTest {

    private static final String LOCK_KEY = "LOCK_KEY";
    public static final long LOCK_WATCHDOG_TIMEOUT = 1_000L;

    RedissonClient redisson;
    GenericContainer<?> redis;

    @BeforeEach
    public void beforeEachTest() {
        redis = createContainer();
        redis.start();

        Config c = createConfig(redis);
        c.setLockWatchdogTimeout(LOCK_WATCHDOG_TIMEOUT);
        c.setLockWatchdogBatchSize(50);
        redisson = Redisson.create(c);
    }

    @AfterEach
    public void afterEachTest() {
        redisson.shutdown();
        redis.stop();
    }

    @Test
    public void testWriteLockAfterTimeout() throws InterruptedException {
        RReadWriteLock rw = redisson.getReadWriteLock(LOCK_KEY);
        RLock lock = rw.writeLock();
        lock.lock();
        try {
            // force expiration renewal error
            restart(redis);
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            assertThatThrownBy(() -> {
                lock.unlock();
            }).isInstanceOf(IllegalMonitorStateException.class);
        }

        RReadWriteLock lock2 = redisson.getReadWriteLock(LOCK_KEY);
        lock2.writeLock().lock();
        try {
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            lock2.writeLock().unlock();
        }

        Thread.sleep(1000);

        lock2.writeLock().lock();
        try {
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            lock2.writeLock().unlock();
        }
    }

    @Test
    public void testReadLockAfterTimeout() throws InterruptedException {
        RReadWriteLock rw = redisson.getReadWriteLock(LOCK_KEY);
        RLock lock = rw.readLock();
        lock.lock();
        try {
            // force expiration renewal error
            restart(redis);
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            assertThatThrownBy(() -> {
                lock.unlock();
            }).isInstanceOf(IllegalMonitorStateException.class);
        }

        RReadWriteLock lock2 = redisson.getReadWriteLock(LOCK_KEY);
        lock2.readLock().lock();
        try {
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            lock2.readLock().unlock();
        }
    }

    @Test
    public void testLockAfterTimeout() throws InterruptedException {
        RLock lock = redisson.getLock(LOCK_KEY);
        lock.lock();
        try {
            // force expiration renewal error
            restart(redis);
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            assertThatThrownBy(lock::unlock).isInstanceOf(IllegalMonitorStateException.class);
        }

        RLock lock2 = redisson.getLock(LOCK_KEY);
        lock2.lock();
        try {
            // wait for timeout
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 2);
        } finally {
            lock2.unlock();
        }
    }
    
    @Test
    public void testLockReentrantRenew() throws InterruptedException {
        RLock lock = redisson.getLock(LOCK_KEY);
        lock.lock();
        lock.lock();
        lock.unlock();
        lock.unlock();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(()->{
            RedissonLock lock2 = (RedissonLock) redisson.getLock(LOCK_KEY);
            lock2.lock();
            while (true) {
                // doSomething
            }
        });
        
        try {
            Thread.sleep(LOCK_WATCHDOG_TIMEOUT * 10);
        } finally {
            // lock2 should hold the lock
            RMap<Object, Object> map = redisson.getMap(LOCK_KEY);
            assertThat(map.isExists()).isTrue();

            executor.shutdownNow();
        }
    }

    @Test
    public void testLockMaxHoldTime() throws InterruptedException {
        Config c = createConfig(redis);
        c.setLockWatchdogTimeout(LOCK_WATCHDOG_TIMEOUT);
        c.setLockMaxHoldTime(2_000);
        RedissonClient client = Redisson.create(c);
        try {
            RLock lock = client.getLock("maxHoldLock");
            lock.lock();

            // renewal stops once the 2s budget is exhausted
            // and the lock is released through the standard unlock path
            Thread.sleep(4_000);

            assertThat(lock.isLocked()).isFalse();
            assertThatThrownBy(lock::unlock).isInstanceOf(IllegalMonitorStateException.class);

            RLock lock2 = client.getLock("maxHoldLock");
            assertThat(lock2.tryLock()).isTrue();
            lock2.unlock();
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testLockMaxHoldTimeReentrant() throws InterruptedException {
        Config c = createConfig(redis);
        c.setLockWatchdogTimeout(LOCK_WATCHDOG_TIMEOUT);
        c.setLockMaxHoldTime(2_000);
        RedissonClient client = Redisson.create(c);
        try {
            RLock lock = client.getLock("maxHoldReentrantLock");
            lock.lock();

            Thread.sleep(1_500);
            // reentrant acquisition doesn't move the budget start
            lock.lock();
            assertThat(lock.getHoldCount()).isEqualTo(2);

            Thread.sleep(2_000);

            // past the original 2s budget, the lock is fully released despite reentrancy
            assertThat(lock.isLocked()).isFalse();

            RLock lock2 = client.getLock("maxHoldReentrantLock");
            assertThat(lock2.tryLock()).isTrue();
            lock2.unlock();
        } finally {
            client.shutdown();
        }
    }

}
