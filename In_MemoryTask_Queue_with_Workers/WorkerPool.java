package In_MemoryTask_Queue_with_Workers;

import In_MemoryTask_Queue_with_Workers.WorkerPool.PoolState;
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

    private volatile PoolState state;
     private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 2000;

    public WorkerPool(int workerCount) {
        this.taskQueue = new TaskQueue();
        this.workers = new ArrayList<>();
        this.workerThreads = new ArrayList<>();
        this.state = PoolState.RUNNING;
        this.retryScheduler = Executors.newScheduledThreadPool(2);
        this.state = PoolState.RUNNING;

        for (int i = 1; i <= workerCount; i++) {
            Worker worker = new Worker(taskQueue,  this , "Worker-" + i);
            Thread thread = new Thread(worker, "Worker-Thread-" + i);
            workers.add(worker);
            workerThreads.add(thread);
            thread.start();
        }
    }

    public void submit(String id, int priority, int maxRetries  , Runnable task) {
        if (state == PoolState.SHUTDOWN) {
            System.out.println("Submission rejected (" + id + "): Pool is SHUTDOWN.");
            return;
        }
        taskQueue.submit(id, priority, maxRetries , task);
    }

    public void cancel(String taskId) {
        taskQueue.cancel(taskId);
    }
    public void handleTaskFailure(Task task, Exception e) {
        if (task.getState() == Task.State.CANCELLED) {
            System.out.println("[Retry] Task " + task.getId() + " was cancelled. Discarding retry.");
            return;
        }

        if (task.canRetry()) {
            task.setState(Task.State.RETRYING);
            long delay = task.calculateBackoffDelay(INITIAL_BACKOFF_MS, MAX_BACKOFF_MS);
            System.out.println("[Retry] Task " + task.getId() + " retrying in " + delay + "ms (Attempt " + task.getAttemptCount() + " failed)...");
            
            retryScheduler.schedule(() -> {
                if (state != PoolState.SHUTDOWN && task.getState() != Task.State.CANCELLED) {
                    taskQueue.resubmit(task);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } else {
            task.setState(Task.State.FAILED);
            System.out.println("[Failure] Task " + task.getId() + " permanently FAILED after " + task.getAttemptCount() + " attempts.");
        }
    }

   

    public void shutdown() {
        state = PoolState.SHUTDOWN;
        retryScheduler.shutdownNow();
        for (Worker worker : workers) {
            worker.stopWorker();
        }
        for (Thread thread : workerThreads) {
            thread.interrupt();
        }
    }

    public PoolState getState() {
        return state;
    }
}
