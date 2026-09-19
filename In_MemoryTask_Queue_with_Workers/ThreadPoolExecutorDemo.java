package In_MemoryTask_Queue_with_Workers;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolExecutorDemo {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Phase 7: ThreadPoolExecutor Exploration ===\n");

        // 1. Configure a Custom ThreadFactory for meaningful thread names
        ThreadFactory customThreadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Executor-Worker-" + counter.getAndIncrement());
                t.setDaemon(false);
                return t;
            }
        };

        // 2. Initialize ThreadPoolExecutor with core, max, keep-alive, bounded queue, and rejection policy
        int corePoolSize = 2;
        int maxPoolSize = 4;
        long keepAliveTime = 10;
        int queueCapacity = 2;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                customThreadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() // Try AbortPolicy, DiscardPolicy, etc.
        );

        System.out.println("-> Executor initialized. Core: " + corePoolSize + ", Max: " + maxPoolSize + ", Queue Capacity: " + queueCapacity);

        // 3. Test execute() vs submit() & Future cancellation
        System.out.println("\n--- Test 1: Submit Tasks and Future Cancellation ---");
        
        // fire-and-forget via execute()
        executor.execute(() -> {
            System.out.println(Thread.currentThread().getName() + " executing execute() task.");
        });

        // submit() returning a Future
        Future<?> futureTask = executor.submit(() -> {
            try {
                System.out.println(Thread.currentThread().getName() + " executing long-running submit() task...");
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + " was interrupted during sleep.");
                Thread.currentThread().interrupt();
            }
        });

        // Submit a task meant to be cancelled before or during execution
        Future<?> cancelableFuture = executor.submit(() -> {
            System.out.println(Thread.currentThread().getName() + " running task to be cancelled.");
        });

        // Test cancellation mechanics
        boolean cancelled = cancelableFuture.cancel(true);
        System.out.println("[Future] cancel() called on cancelableFuture. Success: " + cancelled);
        System.out.println("[Future] isCancelled: " + cancelableFuture.isCancelled());
        System.out.println("[Future] isDone: " + cancelableFuture.isDone());

        // 4. Test Queue Saturation & Rejection Policies
        System.out.println("\n--- Test 2: Queue Saturation & Rejection Policies ---");
        // Flood the executor to force queue filling and thread scaling up to maxPoolSize
        for (int i = 1; i <= 8; i++) {
            final int taskId = i;
            try {
                executor.submit(() -> {
                    System.out.println(Thread.currentThread().getName() + " running Saturation-Task-" + taskId);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                System.out.println("Submitted Saturation-Task-" + taskId);
            } catch (RejectedExecutionException e) {
                System.out.println("REJECTED: Saturation-Task-" + taskId + " due to full queue and max pool size.");
            }
        }

        // Wait for the long-running task to finish
       try {
            futureTask.get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            System.out.println("Task execution failed: " + e.getCause());
        } catch (TimeoutException e) {
            System.out.println("Task timed out before completing.");
        } catch (InterruptedException e) {
            System.out.println("Main thread was interrupted.");
            Thread.currentThread().interrupt();
        }

        // 5. Test Graceful Lifecycle Shutdown
        System.out.println("\n--- Test 3: Lifecycle Shutdown & Await Termination ---");
        executor.shutdown(); // Stop accepting new tasks, drain existing ones
        
        boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        if (terminated) {
            System.out.println("Executor successfully terminated all threads and drained queue.");
        } else {
            System.out.println("Executor did not terminate gracefully in time. Forcing shutdownNow()...");
            executor.shutdownNow();
        }

        System.out.println("\n--- Phase 7 Comparison Complete ---");
    }
}