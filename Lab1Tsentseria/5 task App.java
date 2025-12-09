import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class App {

    private static final String MAP_NAME = "likes-map";
    private static final String KEY = "likes";

    private static final int THREAD_COUNT = 10;
    private static final int INCREMENTS_PER_THREAD = 10_000;

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Integer> map = hz.getMap(MAP_NAME);

        map.put(KEY, 0);

        System.out.println("===== PESSIMISTIC LOCK COUNTER =====");
        System.out.println("Expected increments: " + (THREAD_COUNT * INCREMENTS_PER_THREAD)
                + " for key '" + KEY + "'");

        long startTime = System.currentTimeMillis();

        Thread[] threads = new Thread[THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < INCREMENTS_PER_THREAD; i++) {
                    incrementCounterWithLock(map, KEY);
                }
            });
            threads[t].start();
        }

        for (int t = 0; t < THREAD_COUNT; t++) {
            threads[t].join();
        }

        long endTime = System.currentTimeMillis();

        long expected = (long) THREAD_COUNT * INCREMENTS_PER_THREAD;
        Integer actual = map.get(KEY);

        System.out.println("====================================");
        System.out.println("Expected value: " + expected);
        System.out.println("Actual value:   " + actual);
        System.out.println("Elapsed time (ms): " + (endTime - startTime));
        System.out.println("====================================");

        hz.shutdown();
    }

    private static void incrementCounterWithLock(IMap<String, Integer> map, String key) {
        map.lock(key);
        try {
            Integer current = map.get(key);
            if (current == null) {
                current = 0;
            }
            Integer next = current + 1;
            map.put(key, next);
        } finally {
            map.unlock(key);
        }
    }
}
