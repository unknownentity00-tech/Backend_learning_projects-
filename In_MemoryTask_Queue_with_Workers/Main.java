package In_MemoryTask_Queue_with_Workers;

import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        int workerCount = 2; // Increased to test concurrency alongside priority & retry
        WorkerPool pool = new WorkerPool(workerCount);

        AtomicInteger activeCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        System.out.println("=== Phase 6 Concurrency, Priority, Cancellation & Retry Test ===");

        // Submit tasks with explicit priorities and maxRetries configuration
        pool.submit("Task-1", 1, 2, createWrappedTask(1, activeCount, maxConcurrent, false));
        pool.submit("Task-2", 10, 2, createWrappedTask(2, activeCount, maxConcurrent, false));
        pool.submit("Task-3", 5, 2, createWrappedTask(3, activeCount, maxConcurrent, false));
        pool.submit("Task-4", 8, 2, createWrappedTask(4, activeCount, maxConcurrent, false));
        
        // Add a flaky task that fails on attempt 1, succeeds on retry
        pool.submit("Task-Flaky", 9, 3, createFlakyWrappedTask("Task-Flaky", activeCount, maxConcurrent));

        // Test cancellation: Cancel Task-4 before it executes
        System.out.println("\n[Test Action] Cancelling Task-4...");
        pool.cancel("Task-4");

        Thread.sleep(5000);
        pool.shutdown();

        System.out.println("\n--- Test Results ---");
        System.out.println("Max concurrent executions observed: " + maxConcurrent.get() + " (Must be <= " + workerCount + ")");
    }

    // Helper method to wrap standard task execution logic with concurrency counters
    private static Runnable createWrappedTask(int taskId, AtomicInteger activeCount, AtomicInteger maxConcurrent, boolean shouldFail) {
        return () -> {
            int currentActive = activeCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, currentActive));

            System.out.println(Thread.currentThread().getName() + " → Executing Task-" + taskId + " (Active: " + currentActive + ")");
            try {
                Thread.sleep(300); // Simulate workload duration
                if (shouldFail) {
                    throw new RuntimeException("Forced failure for Task-" + taskId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                activeCount.decrementAndGet();
            }
        };
    }

    // Helper method for testing retries with transient failures
    private static Runnable createFlakyWrappedTask(String taskName, AtomicInteger activeCount, AtomicInteger maxConcurrent) {
        AtomicInteger attemptTracker = new AtomicInteger(0);
        return () -> {
            int attempt = attemptTracker.incrementAndGet();
            int currentActive = activeCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, currentActive));

            System.out.println(Thread.currentThread().getName() + " → Executing " + taskName + " on Attempt " + attempt);
            try {
                Thread.sleep(200);
                if (attempt < 3) {
                    System.out.println("-> " + taskName + " failing intentionally on attempt " + attempt);
                    throw new RuntimeException("Transient failure");
                }
                System.out.println("-> " + taskName + " succeeded on attempt " + attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                activeCount.decrementAndGet();
            }
        };
    }
}