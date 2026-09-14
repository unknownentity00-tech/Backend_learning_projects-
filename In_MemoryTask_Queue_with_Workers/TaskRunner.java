package In_MemoryTask_Queue_with_Workers;

public class TaskRunner implements Runnable {
    private final int taskCount;
    private final WorkerPool workerPool;
    private final String name;

    public TaskRunner(WorkerPool workerPool, int taskCount, String name) {
        this.workerPool = workerPool;
        this.taskCount = taskCount;
        this.name = name;
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= taskCount; i++) {
                String taskId = name + "-Task-" + i;
                int priority = 5;
                int maxRetries = 3; // Pass max retries for Phase 6

                workerPool.submit(taskId, priority, maxRetries, () -> {
                    System.out.println(Thread.currentThread().getName() + " executing " + taskId);
                });

                System.out.println("Producer " + name + ": submitted " + taskId);
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}