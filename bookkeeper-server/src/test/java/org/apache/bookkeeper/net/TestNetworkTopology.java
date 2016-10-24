package org.apache.bookkeeper.net;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Test Case for network topology
 */
public class TestNetworkTopology {

    HashedWheelTimer timer;

    @Before
    public void setup() {
        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build());
        timer.start();
    }

    @After
    public void teardown() {
        timer.stop();
    }

    @Test(timeout = 60000)
    public void testStabilizeNetworkTopology() throws Exception {
        int stabilizeSeconds = 2;
        NodeBase node = new NodeBase("/test/stabilize/network");
        StabilizeNetworkTopology topology = new StabilizeNetworkTopology(timer, stabilizeSeconds);
        topology.add(node);
        assertTrue(topology.contains(node));
        topology.remove(node);
        assertTrue(topology.contains(node));
        TimeUnit.SECONDS.sleep(stabilizeSeconds + stabilizeSeconds / 2);
        assertFalse(topology.contains(node));
    }

    @Test(timeout = 60000)
    public void testNodeAddedAfterRemovedInStabilizedNetworkTopology() throws Exception {
        int stabilizeSeconds = 2;
        final NodeBase node = new NodeBase("/test/node/added/after/removed");
        final StabilizeNetworkTopology topology = new StabilizeNetworkTopology(timer, stabilizeSeconds);
        topology.add(node);
        assertTrue(topology.contains(node));

        topology.remove(node);
        topology.add(node);

        TimeUnit.SECONDS.sleep(stabilizeSeconds + stabilizeSeconds / 2);
        assertTrue(topology.contains(node));
    }

    @Test(timeout = 60000)
    public void testFlappingNodeInStabilizeNetworkTopology() throws Exception {
        int stabilizeSeconds = 2;
        final NodeBase node = new NodeBase("/test/flappingnode/in/stabilized/network/topology");
        final StabilizeNetworkTopology topology = new StabilizeNetworkTopology(timer, stabilizeSeconds);
        topology.add(node);
        assertTrue(topology.contains(node));

        final AtomicInteger counter = new AtomicInteger(0);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int count = counter.incrementAndGet();
                if (count % 2 == 0) {
                    topology.remove(node);
                } else {
                    topology.add(node);
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);

        TimeUnit.SECONDS.sleep(stabilizeSeconds + stabilizeSeconds / 2);
        assertTrue(topology.contains(node));

        executor.shutdown();
    }
}
