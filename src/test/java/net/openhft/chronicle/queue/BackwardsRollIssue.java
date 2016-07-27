/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;

public class BackwardsRollIssue extends ChronicleQueueTestBase {
    private static final int BYTES_LENGTH = 256;
    private static final int BLOCK_SIZE = 256 << 20;
    private static final int NUMBER_OF_TAILERS = 2;
    private static final long INTERVAL_US = 25;

    @Test()
    public void doTest() throws IOException, InterruptedException {
        String path = getTmpDir() + "/backRoll.q";
        new File(path).deleteOnExit();

        AtomicLong counter = new AtomicLong();

        Runnable reader = () -> {
            final String name = Thread.currentThread().getName();
            final AffinityLock rlock = AffinityLock.acquireLock();
            final Bytes bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            try (RollingChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build()) {

                final ExcerptTailer tailer = rqueue.createTailer();

                long lastIdx = tailer.index();
                long lastN = 0;
                System.out.println("first index is: " + lastIdx);
                while (!Thread.interrupted()) {
                    if (tailer.readBytes(bytes)) {

                        long currIdx = tailer.index();

                        if (lastIdx == 0)
                            System.out.println(name + " first index is: " + currIdx);

                        if (currIdx <= lastIdx)
                            System.err.println(name + " index went backwards from " + lastIdx + " to " + currIdx);

                        long n = bytes.readLong();
                        if (n <= lastN)
                            System.out.println("n did not go up! " + n + " last: " + lastN);

                        lastIdx = currIdx;
                        lastN = n;
                        counter.incrementAndGet();
                    }
                }
            } finally {
                if (rlock != null) {
                    rlock.release();
                }
                System.out.printf("Read %,d messages", counter.intValue());
            }
        };
        List<Thread> tailers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TAILERS; i++) {
            Thread tailerThread = new Thread(reader, "tailer-thread-"+i);
            tailers.add(tailerThread);
        }

        long runs = 2_000_000;

        Thread appenderThread = new Thread(() -> {
            final AffinityLock wlock = AffinityLock.acquireLock();
            try {
                ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                        .wireType(WireType.FIELDLESS_BINARY)
                        .rollCycle(TEST_SECONDLY)
                        .blockSize(BLOCK_SIZE)
                        .build();

                final ExcerptAppender appender = wqueue.acquireAppender();
                final Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < runs; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bytes.readPositionRemaining(0, BYTES_LENGTH);
                    bytes.writeLong(0L, start);

                    appender.writeBytes(bytes);
                    next += INTERVAL_US * 1000;
                }
                wqueue.close();
            } finally {
                if (wlock != null) {
                    wlock.release();
                }
            }
        }, "appender-thread");

        tailers.forEach(Thread::start);
        Jvm.pause(100);

        appenderThread.start();
        appenderThread.join();
        System.out.println("appender is done.");

        //Pause to allow tailer to catch up (if needed)
        for (int i = 0; i < 10; i++) {
            if (runs * NUMBER_OF_TAILERS > counter.get())
                Jvm.pause(Jvm.isDebug() ? 10000 : 1000);
        }

        for (int i = 0; i < 10; i++) {
            for (final Thread tailer : tailers) {
                tailer.interrupt();
                tailer.join(100);
            }
        }

        assertEquals(runs * NUMBER_OF_TAILERS, counter.get());
    }

    @Test()
    public void doTest2() throws IOException, InterruptedException, ExecutionException {
        String path = getTmpDir() + "/backRoll.q";
        new File(path).deleteOnExit();

        AtomicLong counter = new AtomicLong();
        AtomicLong lastIndex = new AtomicLong();

        int runs = 1_000_000;
        final Future appenderFuture = Executors.newSingleThreadExecutor(new NamedThreadFactory
                ("appender-thread")).submit(() -> {
            final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build();

            final ExcerptAppender appender = queue.acquireAppender();

            long next = System.nanoTime() + INTERVAL_US * 1000;
            for (int i = 0; i < runs; i++) {
                while (System.nanoTime() < next)
                    /* busy wait*/ ;
                long start = next;
                try (DocumentContext dc = appender.writingDocument();) {
                    dc.wire().write().int64(start);
                    lastIndex.set(dc.index());
                }

                next += INTERVAL_US * 1000;
            }
            queue.close();
        });

        final Runnable reader = () -> {
            final String name = Thread.currentThread().getName();
            final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build();
            RollCycle rollCycle = queue.rollCycle();

            final ExcerptTailer tailer = queue.createTailer();

            long current;
            long last = tailer.index();
            long firstCycle = queue.firstCycle();

            for (; ; ) {
                try (DocumentContext dc = tailer.readingDocument()) {

                    // this will occur when there is no more data to read
                    if (!dc.isPresent()) {
                        if (appenderFuture.isDone())
                            return;
                        Thread.yield();
                        continue;
                    }

                    // read the data that was written
                    long readData = dc.wire().read().int64();
                    current = dc.index();

                    if (firstCycle == Integer.MAX_VALUE) {
                        firstCycle = queue.firstCycle();
                        System.out.println(name + " first cycle is: " + firstCycle);
                    }

                    if (last == 0)
                        System.out.println(name + " first index is: " + current);
                    else {

                        // its dangerous to make this assumption, "current < last" you have
                        // to look at it like this :

                        int currentCycle = rollCycle.toCycle(current);
                        int lastCycle = rollCycle.toCycle(last);

                        long currentSeq = rollCycle.toSequenceNumber(current);
                        long lastSeq = rollCycle.toSequenceNumber(last);

                        Assert.assertTrue("currentCycle=" + currentCycle + ",lastCycle=" + lastCycle + ",firstCycle=" + firstCycle, currentCycle >= lastCycle);

                        if (currentCycle == lastCycle &&
                                currentSeq != rollCycle.toSequenceNumber(-1) && lastSeq !=
                                rollCycle.toSequenceNumber(-1)) {
                            Assert.assertTrue("currentSeq=" + currentSeq + ",lastSeq=" + lastSeq, currentSeq >= lastSeq);
                        }
                    }
                    last = current;
                    counter.incrementAndGet();
                }
            }
        };

        final Future<?> reader1 = Executors.newSingleThreadExecutor(new NamedThreadFactory
                ("tailer-thread1")).submit(reader);
        final Future<?> reader2 = Executors.newSingleThreadExecutor(new NamedThreadFactory
                ("tailer-thread2")).submit(reader);

        appenderFuture.get();
        System.out.println("appender is done. lastIndex=" + lastIndex);

        // wait for reader 1 and 2
        reader1.get();
        reader2.get();

        assertEquals(runs * NUMBER_OF_TAILERS, counter.get());
    }
}
