package In_MemoryTask_Queue_with_Workers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {
    private final AtomicLong submitted = new AtomicLong(0);
    private final AtomicLong started = new AtomicLong(0);
    private final AtomicLong succeeded = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong cancelled = new AtomicLong(0);
    private final AtomicLong retried = new AtomicLong(0);

    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicInteger maxConcurrentTasks = new AtomicInteger(0);

    public void incrementSubmitted() { submitted.incrementAndGet(); }
    public void incrementStarted() { 
        started.incrementAndGet();
        int current = activeTasks.incrementAndGet();
        maxConcurrentTasks.updateAndGet(max -> Math.max(max, current));
    }
    public void incrementSucceeded() { 
        succeeded.incrementAndGet();
        activeTasks.decrementAndGet();
    }
    public void incrementFailed() { 
        failed.incrementAndGet();
        activeTasks.decrementAndGet();
    }
    public void incrementCancelled() { 
        cancelled.incrementAndGet();
        // If it was active when cancelled or queued, ensure active count drops if needed
    }
    public void incrementRetried() { retried.incrementAndGet(); }

    public void decrementActive() {
        activeTasks.decrementAndGet();
    }

    public void printReport(int workerLimit, long elapsedTimeMs) {
        System.out.println("\n========== FINAL STRESS TEST REPORT ==========");
        System.out.println("Tasks Submitted       : " + submitted.get());
        System.out.println("Total Executions      : " + started.get());
        System.out.println("Successful (Terminal) : " + succeeded.get());
        System.out.println("Failed (Terminal)     : " + failed.get());
        System.out.println("Cancelled (Terminal)  : " + cancelled.get());
        System.out.println("Total Retries         : " + retried.get());
        System.out.println("----------------------------------------------");
        System.out.println("Max Concurrent Tasks  : " + maxConcurrentTasks.get());
        System.out.println("Worker Limit          : " + workerLimit);
        System.out.println("Elapsed Time          : " + elapsedTimeMs + " ms");
        System.out.println("Throughput            : " + (succeeded.get() * 1000.0 / Math.max(1, elapsedTimeMs)) + " tasks/sec");
        System.out.println("==============================================");

        // Invariant Validations
        long totalTerminal = succeeded.get() + failed.get() + cancelled.get();
        boolean noTaskLoss = (totalTerminal == submitted.get());
        boolean concurrencyValid = (maxConcurrentTasks.get() <= workerLimit);

        System.out.println("\n--- Invariant Validations ---");
        System.out.println("Task Accounting (Submitted == Terminal) : " + (noTaskLoss ? "PASS" : "FAIL (Mismatch!)"));
        System.out.println("Concurrency Limit (Max Concurrent <= Limit): " + (concurrencyValid ? "PASS" : "FAIL (Exceeded limit!)"));
        System.out.println("==============================================");
    }
}