import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {

    private static final int THREAD_COUNT = 10;
    private static final int INCREMENTS_PER_THREAD = 10_000;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setClusterName("dev");

        CPSubsystemConfig cpConfig = config.getCPSubsystemConfig();
        cpConfig.setCPMemberCount(3);
        cpConfig.setGroupSize(3);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(config);

        HazelcastInstance hz = hz1;

        try {
            // якщо хочеш, можеш залишити попередні завдання:
            // runCounterWithoutLock(hz);
            // runCounterWithPessimisticLock(hz);
            // runCounterWithOptimisticLock(hz);

            runAtomicLongCounter(hz);
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    // сюди нижче додамо метод runAtomicLongCounter(...)

private static void runAtomicLongCounter(HazelcastInstance hz) throws InterruptedException {
    String counterName = "cpAtomicLikes";
    IAtomicLong counter = hz.getCPSubsystem().getAtomicLong(counterName);
    counter.set(0);

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    long startTime = System.nanoTime();

    for (int i = 0; i < THREAD_COUNT; i++) {
        executor.submit(() -> {
            for (int j = 0; j < INCREMENTS_PER_THREAD; j++) {
                counter.incrementAndGet();
            }
        });
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    long durationNs = System.nanoTime() - startTime;
    long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNs);

    long expected = (long) THREAD_COUNT * INCREMENTS_PER_THREAD;
    long actual = counter.get();

    System.out.println("=== IAtomicLong counter with CP Subsystem ===");
    System.out.println("Threads: " + THREAD_COUNT);
    System.out.println("Increments per thread: " + INCREMENTS_PER_THREAD);
    System.out.println("Expected value: " + expected);
    System.out.println("Actual value:   " + actual);
    System.out.println("Execution time: " + durationMs + " ms");

    counter.destroy();
}




}

