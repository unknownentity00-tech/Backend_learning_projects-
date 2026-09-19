package In_MemoryTask_Queue_with_Workers;

public class Worker implements  Runnable {
     private final TaskQueue taskQueue;
     private final String name;
      private WorkerPool workerPool;
    private volatile boolean isRunning = true;

     public Worker(TaskQueue taskQueue,WorkerPool workerPool ,  String name) {
         this.taskQueue = taskQueue;
          this.workerPool = workerPool;
        this.name = name;
    }
  
@Override 
   public  void run (){
        try{
             while (isRunning && !Thread.currentThread().isInterrupted()) {
                Task task = taskQueue.take();
                if(task.getState() == Task.State.CANCELLED){
                    System.out.println("[" + name + "] Skipping cancelled task " + task.getId() 
                    + " (Priority: " + task.getPriority() + ")");
                    continue;
                }
                
                System.out.println("[" + name + "] Starting execution of " + task.getId() 
                    + " (Priority: " + task.getPriority() + ")");

                try {
                    task.run();
                   System.out.println("[" + name + "] Task-" + task.getId() + " succeeded on attempt " + task.getAttemptCount());

                }catch(Exception e ){
                    System.out.println("[" + name + "] Task-" + task.getId() + " failed on attempt " + task.getAttemptCount());
                    workerPool.handleTaskFailure(task, e);
                }
            }
              
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }

        }

  public void stopWorker(){
    isRunning = false ;
  }

     }



