package fi;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Worker component acting as the boundary trigger for the Financial Indicators integration flow.
 * Invoked concurrently by the reactive RxJava orchestration core scheduler thread.
 */
@Singleton
public class FI_Worker {

    private static final Logger logger = LogManager.getLogger(FI_Worker.class);
    private final FI_App app;

    /**
     * Constructs a new FI_Worker daemon instance with injected workflow orchestrator.
     * 
     * @param app the primary business logic pipeline orchestrator for financial metrics tracking
     */
    @Inject
    public FI_Worker(FI_App app) {
        this.app = app;
    }

    /**
     * Triggers the continuous processing loop of the Financial Indicators data transformation system.
     * Evaluates thread execution results and flushes status tracking codes to the external exchange store.
     */
    public void executeTask() {
        logger.info("FinancialIndicators Worker triggered execution loop...");
        int returnedCode = 1;      // Default status flag representing exception fault boundaries
        int recordsProcessed = 0;  // Track number of committed records dynamically
        
        try {
            // Trigger the underlying automated analytics synchronization thread pipeline
            recordsProcessed = app.run();
            returnedCode = 0;      // Seamless execution completed successfully without operational blockades
            logger.info("Financial Indicators worker loop finalized task assignment with exit code 0.");
        } catch (Exception e) {
            logger.error("A critical boundary exception was intercepted inside the FI synchronization workflow.", e);
            returnedCode = 1;      // Explicitly map failure state into the metrics tracking engine
        } finally {
            // Commit structural state metrics into the centralized external exchange pipeline result file
            ExecutionResultWriter.writeWorkerResult("fi_worker", returnedCode, recordsProcessed);
        }
    }
}
