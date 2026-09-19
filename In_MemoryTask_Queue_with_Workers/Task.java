package In_MemoryTask_Queue_with_Workers;

public  class Task  implements Runnable , Comparable<Task>{
     public enum State { QUEUED, RUNNING, SUCCESS, CANCELLED , RETRYING, FAILED};
     private final String id;
    private final int priority;
    private final long sequenceNumber;
    private final Runnable action;
    private final int maxRetries;
    private volatile State state = State.QUEUED;
    private int attemptCount = 0;
     public Task(String id , int priority , long sequenceNumber , Runnable action, int maxRetries){
        this.id = id;
        this.priority = priority;
        this.sequenceNumber = sequenceNumber;
        this.action = action;
        this.maxRetries =maxRetries;
     }
     
    @Override
    public void run() {
        if (state == State.CANCELLED) {
            return;
        }
        state = State.RUNNING;
        attemptCount++;
        action.run();
        state = State.SUCCESS;
    }
    public void  cancel(){
      if (state == State.QUEUED || state ==state.RETRYING) {
            state = State.CANCELLED;
        }
    }
    
      public boolean  canRetry(){
        return (attemptCount -1) < maxRetries && state != State.CANCELLED;
      }
      public long  calculateBackoffDelay(long initialDelayMs ,   long maxDelayMs){
         int retryIndex = attemptCount - 1;
         
        long delay  = initialDelayMs * (long)Math.pow(2,attemptCount -1);
        return Math.min(delay , maxDelayMs);
      }

    public void setState(State state){
        this.state = state;
    }
    public int getPriority() {
        return priority;
    }
    public State getState() {
        return state;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    public String getId() {
        return id;
    }
   @Override
    public int compareTo(Task other) {
        // Higher priority first (DESC)
        int priorityComparison = Integer.compare(other.priority, this.priority);
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        // If priorities are equal, lower sequence number first (ASC / FIFO)
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }
}
 
