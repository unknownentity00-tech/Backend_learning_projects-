package In_MemoryTask_Queue_with_Workers;

import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        int workerCount = 2; // Increased to test concurrency alongside priority & retry
        WorkerPool pool = new WorkerPool(workerCount);

        AtomicInteger activeCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        System.out.println("=== Phase 6 Concurrency, Priority, Cancellation & Retry Test ===");

       // Test 1: Immediate success
        pool.submit("Task-Success", 5, 2, createWrappedTask("Task-Success", activeCount, maxConcurrent, false));

        // Test 2: Transient failure recovering on attempt 3 (2 retries)
        pool.submit("Task-Flaky", 10, 3, createFlakyWrappedTask("Task-Flaky", activeCount, maxConcurrent, 2));

        // Test 3: Permanent failure exhausting retries (maxRetries = 2 -> 3 total executions)
        pool.submit("Task-PermanentFail", 1, 2, createWrappedTask("Task-PermanentFail", activeCount, maxConcurrent, true));

        // Test 4: Cancellation during retry state check
        pool.submit("Task-CancelRetry", 8, 3, createFlakyWrappedTask("Task-CancelRetry", activeCount, maxConcurrent, 5));
        
        System.out.println("\n[Test Action] Cancelling Task-CancelRetry...");
        pool.cancel("Task-CancelRetry");

        Thread.sleep(6000);
        pool.shutdown();

        System.out.println("\n--- Test Results ---");
        System.out.println("Max concurrent executions observed: " + maxConcurrent.get() + " (Must be <= " + workerCount + ")");
    }

   private static Runnable createWrappedTask(String name, AtomicInteger activeCount, AtomicInteger maxConcurrent, boolean shouldFail) {
        return () -> {
            int currentActive = activeCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, currentActive));

            System.out.println(Thread.currentThread().getName() + " → Executing " + name + " (Active: " + currentActive + ")");
            try {
                Thread.sleep(200);
                if (shouldFail) {
                    throw new RuntimeException("Forced failure");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                activeCount.decrementAndGet();
            }
        };
    }private static Runnable createFlakyWrappedTask(String name, AtomicInteger activeCount, AtomicInteger maxConcurrent, int failUntilAttempt) {
        AtomicInteger runCounter = new AtomicInteger(0);
        return () -> {
            int attempt = runCounter.incrementAndGet();
            int currentActive = activeCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, currentActive));

            System.out.println(Thread.currentThread().getName() + " → Executing " + name + " (Attempt " + attempt + ")");
            try {
                Thread.sleep(200);
                if (attempt <= failUntilAttempt) {
                    throw new RuntimeException("Transient failure on attempt " + attempt);
                }
                System.out.println("-> " + name + " successfully completed on attempt " + attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                activeCount.decrementAndGet();
            }
        };
    }
}