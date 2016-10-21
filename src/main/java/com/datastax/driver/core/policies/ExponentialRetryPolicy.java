package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;

public class ExponentialRetryPolicy implements RetryPolicy {

	private static final int DEFAULT_BASE_DELAY_MS = 200;
  
	private static final int DEFAULT_MAX_DELAY_MS = 30000;
  
	private final long baseDelayMs;
  
	private final long maxDelayMs;
	
	private final int maxRetries;
	
	private final RetryCondition retryCondition;
  
	public ExponentialRetryPolicy(){
		this(DEFAULT_BASE_DELAY_MS, DEFAULT_MAX_DELAY_MS, RetryCondition.READ_WRITE_TIMEOUTS);
	}
	
	public ExponentialRetryPolicy(long baseDelayMs, long maxDelayMs, RetryCondition retryCondition){
		this(baseDelayMs, maxDelayMs, retryCondition, -1);
	}
	
	public ExponentialRetryPolicy(long baseDelayMs, long maxDelayMs, RetryCondition retryCondition, int maxRetries){
		if (baseDelayMs < 0 || maxDelayMs < 0)
            throw new IllegalArgumentException("Invalid negative delay");
        if (baseDelayMs == 0)
            throw new IllegalArgumentException("baseDelayMs must be strictly positive");
        if (maxDelayMs < baseDelayMs)
            throw new IllegalArgumentException(String.format("maxDelayMs (got %d) cannot be smaller than baseDelayMs (got %d)", maxDelayMs, baseDelayMs));
        if(retryCondition==null){
        	throw new IllegalArgumentException("Invalid retryCondition");
        }
        
		this.baseDelayMs = baseDelayMs;
		this.maxDelayMs = maxDelayMs;
		this.retryCondition = retryCondition;
		this.maxRetries = maxRetries;
	}
	
	@Override
	public void close() {
        // nothing to do
	}

	@Override
	public void init(Cluster cluster) {
        // nothing to do
		
	}

	@Override
	public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
		if(retryCondition.isRetryOnReadTimeout()){
			return retry(cl,nbRetry);
		}else{
			return RetryDecision.rethrow();
		}
	}

	@Override
	public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
		if(retryCondition.isRetryOnWriteTimeout()){
			return retry(cl,nbRetry);
		}else{
			return RetryDecision.rethrow();
		}
	}

	
	private RetryDecision retry(ConsistencyLevel cl, int nbRetry){
		if(maxRetries <0 ||  maxRetries>nbRetry){
			long currentDelay = Math.min(baseDelayMs * (1L << nbRetry), maxDelayMs);
			try{
				Thread.sleep(currentDelay);
				return RetryDecision.retry(cl);
			}catch(Exception e){
				return RetryDecision.rethrow();
			}
		}else{
			return RetryDecision.rethrow();
		}
		
	}
	
	@Override
	public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
		 return RetryDecision.tryNextHost(cl);
	}

	@Override
	public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica,	int nbRetry) {
		 return RetryDecision.tryNextHost(cl);
	}

	public enum RetryCondition{
		READ_TIMEOUT(true,false),WRITE_TIMEOUT(false,true),READ_WRITE_TIMEOUTS(true,true);
		
		private final boolean retryOnReadTimeout;
		private final boolean retryOnWriteTimeout;
		
		private RetryCondition(boolean retryOnReadTimeout, boolean retryOnWriteTimeout){
			this.retryOnReadTimeout = retryOnReadTimeout;
			this.retryOnWriteTimeout = retryOnWriteTimeout;
		}
		
		
		public boolean isRetryOnReadTimeout(){
			return retryOnReadTimeout;
		}
		
		public boolean isRetryOnWriteTimeout(){
			return retryOnWriteTimeout;
		}
	}
}
