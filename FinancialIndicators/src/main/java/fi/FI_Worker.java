package fi;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.worker.AbstractResilientWorker;

/**
 * Worker component acting as the boundary trigger for the Financial Indicators integration flow.
 */
@Singleton
public class FI_Worker extends AbstractResilientWorker {

    private static final Logger logger = LogManager.getLogger(FI_Worker.class);
    private static final String WORKER_NAME = "fi_worker";

    private final FI_App app;
    private final ApiConfig globalConfig;

    @Inject
    public FI_Worker(FI_App app, ApiConfig globalConfig) {
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
