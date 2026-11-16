package acn;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.worker.AbstractResilientWorker;

/**
 * Worker component acting as the boundary trigger for the Active Clients Number integration flow.
 */
@Singleton
public class ACN_Worker extends AbstractResilientWorker {

    private static final Logger logger = LogManager.getLogger(ACN_Worker.class);
    private static final String WORKER_NAME = "acn_worker";

    private final ACN_App app;
    private final ApiConfig globalConfig;

    @Inject
    public ACN_Worker(ACN_App app, ApiConfig globalConfig) {
        this.app = app;
        this.globalConfig = globalConfig;
    }

    @Override
	public String getWorkerName() {
        return WORKER_NAME;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
	public ApiConfig.WorkerConfig getWorkerConfig() {
    	return globalConfig.getWorkerByName(getClass().getSimpleName());
    }

    @Override
    protected int doExecute() throws Exception {
        return app.run();
    }
}
