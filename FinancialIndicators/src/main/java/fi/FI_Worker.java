package fi;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Worker component acting as the boundary trigger for the Financial Indicators integration flow.
 * Invoked concurrently by the reactive RxJava orchestration core scheduler thread.
 * Manages atomic execution states to protect pipeline loops from overlapping concurrency spikes.
 */
@Singleton
public class FI_Worker {

    private static final Logger logger = LogManager.getLogger(FI_Worker.class);
    
    // Thread-safe state indicator providing instant rapid-fire protection to the Rx scheduler loop
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    
    private final FI_App app;
    private final ApiConfig globalConfig;

    /**
     * Constructs a new FI_Worker daemon instance with injected workflow orchestrator.
     */
    @Inject
    public FI_Worker(FI_App app, ApiConfig globalConfig) {
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
     * Triggers the continuous processing loop of the Financial Indicators data transformation system.
     * Natively handles retry loops and delays across multiple attempts until exhaustion or success.
     */
    public void executeTask() {
        // Enforce immediate cancellation if another scheduler tick slips through concurrently
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("FI_Worker task execution rejected. Process channel state is already occupied.");
            return;
        }

        logger.info("FinancialIndicators Worker triggered execution loop...");
        int returnedCode = 1;      // Default status flag representing exception fault boundaries
        int recordsProcessed = 0;  // Track number of committed records dynamically

        // Extract runtime retry limits from the central application.yaml configurations
        int maxAttempts = 3;
        long retryDelaySeconds = 5; // Default safe fallback
        
        ApiConfig.WorkerConfig fiConfig = globalConfig.getFiWorker();
        if (fiConfig != null) {
            if (fiConfig.getMaxAttempts() > 0) {
                maxAttempts = fiConfig.getMaxAttempts();
            }
            // Fix: Using initialTimeoutMs from ApiConfig and converting milliseconds to seconds
            if (fiConfig.getInitialTimeoutMs() > 0) {
                retryDelaySeconds = TimeUnit.MILLISECONDS.toSeconds(fiConfig.getInitialTimeoutMs());
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
                    logger.info("Initiating FI integration attempt {} of {}...", currentAttempt, maxAttempts);
                    
                    // Trigger the underlying automated analytics synchronization thread pipeline
                    recordsProcessed = app.run();
                    
                    returnedCode = 0; // Seamless execution completed successfully without operational blockades
                    success = true;
                    logger.info("Financial Indicators worker loop finalized task assignment with exit code 0.");
                } catch (Exception attemptException) {
                    logger.error("FI pipeline synchronization error on attempt {}/{} - Message: {}", 
                            currentAttempt, maxAttempts, attemptException.getMessage());
                    
                    // If we have remaining available retries, stand by for the configured delay period
                    if (currentAttempt < maxAttempts) {
                        logger.info("Sleeping for {} seconds before starting the next task execution retry attempt...", retryDelaySeconds);
                        TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    } else {
                        // Explicitly map failure state into the metrics tracking engine as all recovery attempts are now officially exhausted
                        returnedCode = 1;
                        logger.error("All scheduled FI execution retries exhausted. Registering core operational breakdown.");
                    }
                }
            }
        } catch (InterruptedException ie) {
            logger.warn("FI internal retry delay thread context was interrupted. Terminating active execution sequence.");
            Thread.currentThread().interrupt();
            returnedCode = 1;
        } finally {
            try {
                // Commit structural state metrics into the centralized external exchange pipeline result file
                ExecutionResultWriter.writeWorkerResult("fi_worker", returnedCode, recordsProcessed);
            } finally {
                // Absolutely vital: always release the execution block so subsequent interval cycles can run
                isRunning.set(false);
                logger.info("FI_Worker pipeline context released. Ready for future integration requests.");
            }
        }
    }
}
