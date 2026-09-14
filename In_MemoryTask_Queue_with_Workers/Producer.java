package In_MemoryTask_Queue_with_Workers;

public class Producer implements Runnable {
    private final WorkerPool workerPool;
    private final int taskCount;
    private final String name;

    public Producer(WorkerPool workerPool, int taskCount, String name) {
        this.workerPool = workerPool;
        this.taskCount = taskCount;
        this.name = name;
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= taskCount; i++) {
                String taskId = name + "-Task-" + i;
                int priority = (i % 5) + 1; 
                int maxRetries = 2; // Pass max retries for Phase 6

                workerPool.submit(taskId, priority, maxRetries, () -> {
                    System.out.println(Thread.currentThread().getName() + " → Executing " + taskId + " (Priority: " + priority + ")");
                    // Simulate occasional failure for testing retries
                    if (Math.random() < 0.3) {
                        throw new RuntimeException("Random transient failure");
                    }
                });

                System.out.println("Producer " + name + ": submitted " + taskId + " with priority " + priority);
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}