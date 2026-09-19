package In_MemoryTask_Queue_with_Workers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WorkerPool {
    public enum PoolState { RUNNING, SHUTDOWN }

    private final TaskQueue taskQueue;
    private final List<Worker> workers;
    private final List<Thread> workerThreads;
    private final ScheduledExecutorService retryScheduler;
    private final MetricsCollector metrics; // Field declared here
    private volatile PoolState state;

    private static final long INITIAL_BACKOFF_MS = 50;
    private static final long MAX_BACKOFF_MS = 500;

    public WorkerPool(int workerCount) {
        this.taskQueue = new TaskQueue();
        this.metrics = new MetricsCollector(); // Initialized here
        this.workers = new ArrayList<>();
        this.workerThreads = new ArrayList<>();
        this.retryScheduler = Executors.newScheduledThreadPool(2);
        this.state = PoolState.RUNNING;

        for (int i = 1; i <= workerCount; i++) {
            Worker worker = new Worker(taskQueue, this, metrics, "Worker-" + i);
            Thread thread = new Thread(worker, "Worker-Thread-" + i);
            workers.add(worker);
            workerThreads.add(thread);
            thread.start();
        }
    }

    public void submit(String id, int priority, int maxRetries, Runnable task) {
        if (state == PoolState.SHUTDOWN) {
            return;
        }
        metrics.incrementSubmitted();
        taskQueue.submit(id, priority, maxRetries, task);
    }

    public void handleTaskFailure(Task task, Exception e) {
        if (task.getState() == Task.State.CANCELLED) {
            return;
        }

        if (task.canRetry()) {
            task.setState(Task.State.RETRYING);
            metrics.incrementRetried();
            long delay = task.calculateBackoffDelay(INITIAL_BACKOFF_MS, MAX_BACKOFF_MS);
            
            retryScheduler.schedule(() -> {
                if (state != PoolState.SHUTDOWN && task.getState() != Task.State.CANCELLED) {
                    taskQueue.resubmit(task);
                } else if (task.getState() == Task.State.CANCELLED) {
                    metrics.incrementCancelled();
                }
            }, delay, TimeUnit.MILLISECONDS);
        } else {
            task.setState(Task.State.FAILED);
            metrics.incrementFailed();
        }
    }

    public void cancel(String taskId) {
        boolean cancelled = taskQueue.cancel(taskId);
        if (cancelled) {
            metrics.incrementCancelled();
        }
    }

    public void shutdown() {
        state = PoolState.SHUTDOWN;
        retryScheduler.shutdown();
        for (Worker worker : workers) {
            worker.stopWorker();
        }
        for (Thread thread : workerThreads) {
            thread.interrupt();
        }
    }

    public MetricsCollector getMetrics() {
        return metrics;
    }

    public PoolState getState() {
        return state;
    }
}