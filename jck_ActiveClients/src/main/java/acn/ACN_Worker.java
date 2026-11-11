package acn;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Worker component acting as the boundary trigger for the Active Clients Number integration flow.
 * Manages atomic execution states and handles resilient exponential random backoff retry policies with Jitter.
 */
@Singleton
public class ACN_Worker {

    private static final Logger logger = LogManager.getLogger(ACN_Worker.class);
    
    // Thread-safe state indicator providing instant rapid-fire protection to the Rx scheduler loop
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    
    private final ACN_App app;
    private final ApiConfig globalConfig;

    @Inject
    public ACN_Worker(ACN_App app, ApiConfig globalConfig) {
        this.app = app;
        this.globalConfig = globalConfig;
    }

    /**
     * Exposes the active pipeline runtime state directly to external reactive orchestration filters.
     */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Triggers the continuous processing loop of the Active Clients data transformation system.
     * Evaluates execution limits against max_timeout_ms and applies Resilience4j-style Jitter parameters natively.
     */
    public void executeTask() {
        // Enforce immediate cancellation if another scheduler tick slips through concurrently
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("ACN_Worker task execution rejected. Process channel state is already occupied.");
            return;
        }

        logger.info("ActiveClientsNumber Worker triggered execution loop...");
        int returnedCode = 1;      // Default status flag representing exception faults
        int recordsProcessed = 0;  // Track number of committed records dynamically

        // Extract runtime time-based budget limits from the central configurations
        long maxTimeoutMs = 120000;       // Default 2 minutes fallback budget
        long initialTimeoutMs = 10000;     // Default 10 seconds base delay
        double multiplier = 2.0;          // Standard Resilience4j multiplier coefficient
        double randomizationFactor = 0.5; // Jitter parameter to prevent thundering herd spikes
        
        ApiConfig.WorkerConfig acnConfig = globalConfig.getAcnWorker();
        if (acnConfig != null) {
            if (acnConfig.getMaxTimeoutMs() > 0) {
                maxTimeoutMs = acnConfig.getMaxTimeoutMs();
            }
            if (acnConfig.getInitialTimeoutMs() > 0) {
                initialTimeoutMs = acnConfig.getInitialTimeoutMs();
            }
        }

        long startTimeMillis = System.currentTimeMillis();
        int attempt = 0;
        boolean success = false;

        try {
            // Loop until a successful pipeline integration occurs or cumulative time limits wear out completely
            while (!success) {
                attempt++;
                long timeElapsed = System.currentTimeMillis() - startTimeMillis;
                
                // Absolute deadline validation check before spawning a new execution attempt
                if (timeElapsed >= maxTimeoutMs) {
                    logger.error("ACN resilience budget exhausted. Total elapsed time {} ms exceeds max_timeout_ms {} ms. Aborting integration job.", timeElapsed, maxTimeoutMs);
                    returnedCode = 1;
                    break;
                }

                try {
                    logger.info("Initiating ACN integration attempt {} (Time elapsed: {} ms / Budget: {} ms)...", attempt, timeElapsed, maxTimeoutMs);
                    
                    // Trigger the core processing engine pipeline
                    recordsProcessed = app.run();
                    
                    returnedCode = 0; // Success
                    success = true;
                    logger.info("Active Clients worker loop finalized task assignment with exit code 0.");
                } catch (Exception attemptException) {
                    timeElapsed = System.currentTimeMillis() - startTimeMillis;
                    logger.error("ACN pipeline synchronization error on attempt {} - Message: {}", attempt, attemptException.getMessage());

                    // Resilience4j: Current Interval = initial_interval * (multiplier)^(attempt - 1)
                    long currentInterval = (long) (initialTimeoutMs * Math.pow(multiplier, attempt - 1));
                    
                    // Resilience4j Jitter calculation: Retry Delay = Current Interval ± (Current Interval * randomization_factor)
                    long delta = (long) (currentInterval * randomizationFactor);
                    long minDelay = currentInterval - delta;
                    long maxDelay = currentInterval + delta;
                    long nextDelayMs = ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1);
                    
                    // Cap the randomized delay so it doesn't sleep past the total remaining maxTimeout budget
                    long remainingBudgetMs = maxTimeoutMs - timeElapsed;
                    if (nextDelayMs > remainingBudgetMs) {
                        nextDelayMs = remainingBudgetMs;
                    }

                    if (nextDelayMs > 0) {
                        logger.info("Resilience4j backoff with Jitter: Standby for {} ms (Base interval: {} ms) before next ACN retry attempt...", nextDelayMs, currentInterval);
                        TimeUnit.MILLISECONDS.sleep(nextDelayMs);
                    } else {
                        returnedCode = 1;
                        break;
                    }
                }
            }
        } catch (InterruptedException ie) {
            logger.warn("ACN internal retry delay thread context was interrupted. Terminating active execution sequence.");
            Thread.currentThread().interrupt();
            returnedCode = 1;
        } finally {
            try {
                // Commit structural state metrics into the centralized external exchange pipeline result file
                ExecutionResultWriter.writeWorkerResult("acn_worker", returnedCode, recordsProcessed);
            } finally {
                // Absolutely vital: always release the execution block so subsequent interval cycles can run
                isRunning.set(false);
                logger.info("ACN_Worker pipeline context released. Ready for future integration requests.");
            }
        }
    }
}
