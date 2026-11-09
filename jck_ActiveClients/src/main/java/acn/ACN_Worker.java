package acn;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Worker component acting as the boundary trigger for the Active Clients Number integration flow.
 */
@Singleton
public class ACN_Worker {

    private static final Logger logger = LogManager.getLogger(ACN_Worker.class);
    private final ACN_App app;

    @Inject
    public ACN_Worker(ACN_App app) {
        this.app = app;
    }

    /**
     * Triggers the continuous processing loop of the Active Clients data transformation system.
     */
    public void executeTask() {
        logger.info("ActiveClientsNumber Worker triggered execution loop...");
        int returnedCode = 1;      // Default status flag representing exception faults
        int recordsProcessed = 0;  // Track number of committed records dynamically
        
        try {
            // Trigger the pipeline and capture the total number of synchronized records
            recordsProcessed = app.run();
            returnedCode = 0;      // Execution completed successfully without operational blockades
            logger.info("Active Clients worker loop finalized task assignment with exit code 0.");
        } catch (Exception e) {
            logger.error("A critical boundary exception was intercepted inside the ACN synchronization workflow.", e);
            returnedCode = 1;      // Map failure state into metrics tracking engine
        } finally {
            // Commit structural state metrics into the centralized external exchange pipeline result file
            ExecutionResultWriter.writeWorkerResult("acn_worker", returnedCode, recordsProcessed);
        }
    }
}
