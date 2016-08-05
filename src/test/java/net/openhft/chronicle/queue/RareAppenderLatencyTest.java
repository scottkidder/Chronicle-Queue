package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by skidder on 8/2/16.
 *
 * Targeting the problem of tailers in different threads where one writes very rarely, and the other
 * nearly constantly.
 *
 * The rare appender will have very bad latency proportional to the number of messages written since
 * it last appended.
 */
public class RareAppenderLatencyTest {
    private final static int HEAVY_MSGS = 1_000_000;
    private final static int RARE_MSGS = 50;


    boolean isAssertionsOn;
    private ExecutorService appenderES;

    @Before
    public void before() {
        appenderES = Executors.newSingleThreadExecutor(new NamedThreadFactory
                ("Appender", false));
    }

    @After
    public void after() {
        appenderES.shutdownNow();
    }


    @Test
    public void testRareAppenderLatency() throws IOException, InterruptedException, ExecutionException {
        System.setProperty("ignoreHeaderCountIfNumberOfBytesBehindExceeds", "" + (1 << 12));

        if (Jvm.isDebug())
            // this is a performance test so should not be run in debug mode
            return;

        assert (isAssertionsOn = true) == true;

        if (isAssertionsOn)
            // this is a performance test so should not be run with assertions turned on
            return;

        String pathname = OS.getTarget() + "/testRareAppenderLatency-" + System.nanoTime();
        new File(pathname).deleteOnExit();

        // Shared queue between two threads appending. One appends very rarely, another heavily.
        ChronicleQueue queue = SingleChronicleQueueBuilder.binary(pathname)
                .rollCycle(RollCycles.HOURLY)
                .build();

        String text = getText();

        // Write a some messages with an appender from Main thread.
        ExcerptAppender rareAppender = queue.acquireAppender();
        for (int i = 0; i < RARE_MSGS; i++) {
            try (DocumentContext ctx = rareAppender.writingDocument()) {
                ctx.wire()
                        .write(() -> "ts").int64(System.nanoTime())
                        .write(() -> "msg").text(text);
            }
        }

        // Write a bunch of messages from another thread.
        Future f = appenderES.submit(() -> {
            ExcerptAppender appender = queue.acquireAppender();
            long start = System.nanoTime();
            for (int i = 0; i < HEAVY_MSGS; i++) {
                try (DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire()
                            .write(() -> "ts").int64(System.nanoTime())
                            .write(() -> "msg").text(text);
                }
                if (appenderES.isShutdown())
                    return;
            }

            long now = System.nanoTime();
            long durUs = (now - start) / 1000;
            System.out.println("Wrote heavy " + HEAVY_MSGS + " msgs in " + durUs + " us");
            System.out.println("Avg " + durUs / HEAVY_MSGS + " us per msg");
        });

        f.get();

        // Write a message from the Main thread again (this will have unacceptable latency!)
        rareAppender = queue.acquireAppender();
        long now = System.nanoTime();
        try (DocumentContext ctx = rareAppender.writingDocument()) {
            ctx.wire()
                    .write(() -> "ts").int64(System.nanoTime())
                    .write(() -> "msg").text(text);
        }
        long l = System.nanoTime() - now;


        // Write another message from the Main thread (this will be fast since we are caught up)
        now = System.nanoTime();
        try (DocumentContext ctx = rareAppender.writingDocument()) {
            ctx.wire()
                    .write(() -> "ts").int64(System.nanoTime())
                    .write(() -> "msg").text(text);
        }
        System.out.println("Wrote first rare one in " + l/1000 + " us");
        System.out.println("Wrote another rare one in " + (System.nanoTime() - now)/1000 + " us");

        assertFalse("Appending from rare thread latency too high!", l/1000 > 50_000);

        ChronicleQueue anotherQ = new SingleChronicleQueueBuilder(pathname)
                .rollCycle(RollCycles.HOURLY)
                .build();

        ExcerptTailer tailer = anotherQ.createTailer();
        int totalMsgs = HEAVY_MSGS + RARE_MSGS + 2;

        long count = 0;
        long lastTs = 0;
        for (; ;) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    break;
                }
                long ts = dc.wire().read(() -> "ts").int64();
                assertTrue("timestamp went backwards! " + ts + " " + lastTs, ts >= lastTs);
                lastTs = ts;

                String msg = dc.wire().read(() -> "msg").text();
                assertTrue("text was wrong length" + msg.length(), msg.length() == 360);

                count++;
            }
        }

        assertEquals(totalMsgs, count);
    }

    @NotNull
    protected String getText() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) sb.append(UUID.randomUUID());
        return sb.toString();
    }
}
