package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.PretouchHandler;
import net.openhft.chronicle.threads.BusyPauser;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by skidder on 2/3/17.
 */
public class PretouchWalkTest {
    @Test
    @Ignore("long running")
    public void testPretoucherWalking() throws Exception {
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(IOTools.tempName("pt_walk")).build();
        EventGroup group = new EventGroup(false, BusyPauser.INSTANCE, true, "pt-walk-test");

        group.addHandler(() -> {
            final ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    final Object hi = dc.wire().read("hi").object();
                    if (hi != null) {
                        // System.out.println(hi.toString());
                    }
                }
            }

            return true;
        });

        group.addHandler(() -> {
            final ExcerptAppender excerptAppender = queue.acquireAppender();

            try (DocumentContext dc = excerptAppender.writingDocument()) {
                dc.wire().write("hi").object(new String("blablablabla"));
            }

            return true;
        });

        group.addHandler(new PretouchHandler(queue));

        group.start();

        final long start = System.currentTimeMillis();
        final long end = start + TimeUnit.MINUTES.toMillis(5);

        while (System.currentTimeMillis() < end) {
            Jvm.pause(1000);
            System.out.println("Waiting...");
        }

    }
}
