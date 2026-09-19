package In_MemoryTask_Queue_with_Workers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class StressTestMain {
    public static void main(String[] args) throws InterruptedException {
        int workerCount = 8;
        int producerCount = 5;
        int tasksPerProducer = 1000;
        int totalTasks = producerCount * tasksPerProducer;

        WorkerPool pool = new WorkerPool(workerCount);
        System.out.println("=== Phase 8: Heavy Stress Test Starting ===");
        System.out.println("Workers: " + workerCount + " | Producers: " + producerCount + " | Total Tasks: " + totalTasks);

        long startTime = System.currentTimeMillis();

        // 1. Launch Multiple Concurrent Producers
        List<Thread> producerThreads = new ArrayList<>();
        for (int p = 1; p <= producerCount; p++) {
            final int producerId = p;
            Thread pt = new Thread(() -> {
                Random rand = new Random();
                for (int i = 1; i <= tasksPerProducer; i++) {
                    String taskId = "P" + producerId + "-T" + i;
                    int priority = rand.nextInt(10) + 1;
                    int maxRetries = 2;

                    pool.submit(taskId, priority, maxRetries, () -> {
                        // Simulate random transient failures (30% failure rate)
                        if (rand.nextDouble() < 0.3) {
                            throw new RuntimeException("Simulated transient failure");
                        }
                        // Simulate tiny workload duration
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }, "Producer-" + p);
            producerThreads.add(pt);
            pt.start();
        }

        // 2. Launch Background Cancellation Thread
        Thread cancellerThread = new Thread(() -> {
            Random rand = new Random();
            for (int i = 0; i < 50; i++) {
                try {
                    Thread.sleep(20);
                    int p = rand.nextInt(producerCount) + 1;
                    int t = rand.nextInt(tasksPerProducer) + 1;
                    pool.cancel("P" + p + "-T" + t);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "Canceller-Thread");
        cancellerThread.start();

        // Wait for producers to finish submitting
        for (Thread pt : producerThreads) {
            pt.join();
        }
        cancellerThread.join();

        // Let execution drain
        Thread.sleep(4000);
        pool.shutdown();

        long elapsedTime = System.currentTimeMillis() - startTime;

        // Print final verification metrics report
        pool.getMetrics().printReport(workerCount, elapsedTime);
    }
}