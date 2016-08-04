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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertEquals;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;

public class CycleNotFoundTest extends ChronicleQueueTestBase {
    private static final int BLOCK_SIZE = 256 << 20;
    private static final int NUMBER_OF_TAILERS = 8;
    private static final long INTERVAL_US = 25;
    private static final long NUMBER_OF_MSG = 100_000;

    @Ignore("long running test")
    @Test
    public void tailerCycleNotFoundTest() throws IOException, InterruptedException {
        String path = getTmpDir() + "/tailerCycleNotFound.q";
        new File(path).deleteOnExit();

        AtomicLong counter = new AtomicLong();

        Runnable reader = () -> {
            try (RollingChronicleQueue rqueue = SingleChronicleQueueBuilder.binary(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build()) {

                final ExcerptTailer tailer = rqueue.createTailer();

                long lastNum = -1;
                TailerState lastState = tailer.state();
                int lastCycle = -1;
                long lastIndex = -1;

                while (!Thread.interrupted())
                    try (DocumentContext dc = tailer.readingDocument()) {
                        long n;
                        if (!dc.isPresent()) {
                            lastState = tailer.state();
                            continue;
                        } else {
                            n = dc.wire().read().int64();
                        }
                        if (n != lastNum + 1) {
                            if (n <= lastNum) dl("num did not increase! " + n + " lastNum: " + lastNum);
                            else dl("num increased by more than 1! " + n + " lastNum: " + lastNum);

                            dl("lastState: " + lastState + "curr: " + tailer.state());
                            dl("lastCycle: " + lastCycle + "curr: " + tailer.cycle());
                            dl("lastIndex: " + lastIndex + "curr: " + tailer.index());

                            long atLast = readLongAtIndex(rqueue, lastIndex);
                            dl("at lastIndex: " + atLast);
                            long atCurrent = readLongAtIndex(rqueue, tailer.index());
                            dl("at currIndex: " + atCurrent);
                        }

                        lastNum = n;
                        lastState = tailer.state();
                        lastCycle = tailer.cycle();
                        lastIndex = tailer.index();
                        counter.incrementAndGet();
                    } catch (UnsupportedOperationException ignored) {
                    }
            } finally {
                dl("Read " + counter.intValue() + " messages");
            }
        };

        List<Thread> tailers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TAILERS; i++) {
            Thread tailerThread = new Thread(reader, "tailer-thread-" + i);
            tailers.add(tailerThread);
        }

        Thread appenderThread = new Thread(() -> {
            ChronicleQueue wqueue = SingleChronicleQueueBuilder.binary(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build();

            final ExcerptAppender appender = wqueue.acquireAppender();

            long next = System.nanoTime() + INTERVAL_US * 1000;
            for (int i = 0; i < NUMBER_OF_MSG; i++) {
                while (System.nanoTime() < next)
                    /* busy wait*/ ;
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().int64(i);
                }
                next += INTERVAL_US * 1000;
            }
            wqueue.close();
        }, "appender-thread");

        tailers.forEach(Thread::start);
        Jvm.pause(100);

        appenderThread.start();
        appenderThread.join();
        dl("appender is done.");

        //Pause to allow tailer to catch up (if needed)
        for (int i = 0; i < 10; i++) {
            if (NUMBER_OF_MSG * NUMBER_OF_TAILERS > counter.get())
                Jvm.pause(Jvm.isDebug() ? 10000 : 1000);
        }

        for (int i = 0; i < 10; i++) {
            for (final Thread tailer : tailers) {
                tailer.interrupt();
                tailer.join(100);
            }
        }
        Thread.sleep(200);

        assertEquals(NUMBER_OF_MSG * NUMBER_OF_TAILERS, counter.get());
    }

    @NotNull
    protected long readLongAtIndex(RollingChronicleQueue queue, long index) {
        ExcerptTailer tailer1 = queue.createTailer();
        tailer1.moveToIndex(index);
        long reread;
        try (DocumentContext dc1 = tailer1.readingDocument()) {
            reread = dc1.wire().read().int64();
        }
        return reread;
    }

    protected void dl(String msg) {
        System.out.println(Thread.currentThread().getName() + ":" + msg);
    }
}
