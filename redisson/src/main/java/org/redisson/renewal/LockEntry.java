/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.renewal;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.LongConsumer;

/**
 *
 * @author Nikita Koksharov
 *
 */
public class LockEntry {

    final Queue<Long> threadsQueue = new ConcurrentLinkedQueue<>();
    final Map<Long, Integer> threadId2counter = new ConcurrentHashMap<>();
    final Map<Long, String> threadId2lockName = new ConcurrentHashMap<>();
    final Map<Long, Long> threadId2deadline = new ConcurrentHashMap<>();

    private final long maxHoldTime;
    private final LongConsumer expirationListener;

    LockEntry() {
        this(0, null);
    }

    LockEntry(long maxHoldTime, LongConsumer expirationListener) {
        this.maxHoldTime = maxHoldTime;
        this.expirationListener = expirationListener;
    }

    public String getLockName(long threadId) {
        return threadId2lockName.get(threadId);
    }

    public void addThreadId(long threadId, String lockName) {
        threadId2counter.compute(threadId, (t, counter) -> {
            if (counter == null && maxHoldTime > 0) {
                // budget starts at the first acquisition,
                // reentrant acquisitions don't move it
                threadId2deadline.put(threadId, System.currentTimeMillis() + maxHoldTime);
            }
            counter = Optional.ofNullable(counter).orElse(0);
            counter++;
            threadsQueue.add(threadId);
            return counter;
        });
        threadId2lockName.putIfAbsent(threadId, lockName);
    }

    public Long getDeadline(long threadId) {
        return threadId2deadline.get(threadId);
    }

    public LongConsumer getExpirationListener() {
        return expirationListener;
    }

    public boolean hasNoThreads() {
        return threadsQueue.isEmpty();
    }

    public Long getFirstThreadId() {
        return threadsQueue.peek();
    }

    public void removeThreadId(long threadId) {
        threadId2counter.computeIfPresent(threadId, (t, counter) -> {
            counter--;
            if (counter == 0) {
                threadsQueue.removeIf(v-> v == threadId);
                threadId2lockName.remove(threadId);
                threadId2deadline.remove(threadId);
                return null;
            }
            return counter;
        });
    }

    public void removeThread(long threadId) {
        threadId2counter.remove(threadId);
        threadsQueue.removeIf(v -> v == threadId);
        threadId2lockName.remove(threadId);
        threadId2deadline.remove(threadId);
    }

}
