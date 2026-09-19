package In_MemoryTask_Queue_with_Workers;

public class Worker implements Runnable {
    private final TaskQueue taskQueue;
    private final WorkerPool workerPool;
    private final MetricsCollector metrics;
    private final String name;
    private volatile boolean isRunning = true;

    public Worker(TaskQueue taskQueue, WorkerPool workerPool, MetricsCollector metrics, String name) {
        this.taskQueue = taskQueue;
        this.workerPool = workerPool;
        this.metrics = metrics;
        this.name = name;
    }

    @Override
    public void run() {
        try {
            while (isRunning && !Thread.currentThread().isInterrupted()) {
                Task task = taskQueue.take();
                
                if (task.getState() == Task.State.CANCELLED) {
                    continue;
                }

                metrics.incrementStarted();
                try {
                    task.run();
                    metrics.incrementSucceeded();
                } catch (Exception e) {
                    workerPool.handleTaskFailure(task, e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stopWorker() {
        isRunning = false;
    }
}