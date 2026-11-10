package acn;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Worker component acting as the boundary trigger for the Active Clients Number integration flow.
 * Manages atomic execution states to protect pipeline loops from overlapping concurrency spikes.
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
     * Natively handles retry loops and delays across multiple attempts until exhaustion or success.
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

        // Extract runtime retry limits from the central application.yaml configurations
        int maxAttempts = 3;
        long retryDelaySeconds = 5; // Default safe fallback
        
        ApiConfig.WorkerConfig acnConfig = globalConfig.getAcnWorker();
        if (acnConfig != null) {
            if (acnConfig.getMaxAttempts() > 0) {
                maxAttempts = acnConfig.getMaxAttempts();
            }
            // Fix: Using initialTimeoutMs from ApiConfig and converting milliseconds to seconds
            if (acnConfig.getInitialTimeoutMs() > 0) {
                retryDelaySeconds = TimeUnit.MILLISECONDS.toSeconds(acnConfig.getInitialTimeoutMs());
                // Fallback to 1 second if timeout config is lower than 1000ms to avoid 0-second infinite loop spin
                if (retryDelaySeconds == 0) {
                    retryDelaySeconds = 1;
                }
            }
        }

        int currentAttempt = 0;
        boolean success = false;

        try {
            // Loop until a successful pipeline integration occurs or retry attempts wear out completely
            while (currentAttempt < maxAttempts && !success) {
                currentAttempt++;
                try {
                    logger.info("Initiating ACN integration attempt {} of {}...", currentAttempt, maxAttempts);
                    
                    // Trigger the core processing engine pipeline
                    recordsProcessed = app.run();
                    
                    returnedCode = 0; // Success
                    success = true;
                    logger.info("Active Clients worker loop finalized task assignment with exit code 0.");
                } catch (Exception attemptException) {
                    logger.error("ACN pipeline synchronization error on attempt {}/{} - Message: {}", 
                            currentAttempt, maxAttempts, attemptException.getMessage());
                    
                    // If we have remaining available retries, stand by for the configured delay period
                    if (currentAttempt < maxAttempts) {
                        logger.info("Sleeping for {} seconds before starting the next task execution retry attempt...", retryDelaySeconds);
                        TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    } else {
                        // Mark final failure metrics as all recovery attempts are now officially exhausted
                        returnedCode = 1;
                        logger.error("All scheduled ACN execution retries exhausted. Registering core operational breakdown.");
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
