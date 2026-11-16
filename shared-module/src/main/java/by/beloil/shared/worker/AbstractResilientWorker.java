package by.beloil.shared.worker;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.utils.ExecutionResultWriter;

/**
 * Base class providing thread-safe single-instance execution guard,
 * time-budgeted exponential backoff with jitter, and standardized result reporting.
 */
public abstract class AbstractResilientWorker {

    private static final long DEFAULT_MAX_TIMEOUT_MS = 120_000L;
    private static final long DEFAULT_INITIAL_TIMEOUT_MS = 10_000L;
    private static final double MULTIPLIER = 2.0;
    private static final double RANDOMIZATION_FACTOR = 0.5;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Executes the worker task with exponential backoff and jitter within the configured time budget.
     */
    public final void executeTask() {
        Logger log = getLogger();
        String workerName = getWorkerName();

        if (!running.compareAndSet(false, true)) {
            log.warn("[{}] Task execution rejected: worker is already running.", workerName);
            return;
        }

        log.info("[{}] Triggered execution loop...", workerName);

        int exitCode = 1;
        int recordsProcessed = 0;

        ApiConfig.WorkerConfig config = getWorkerConfig();
        long maxTimeoutMs = (config != null && config.getMaxTimeoutMs() > 0)
                ? config.getMaxTimeoutMs()
                : DEFAULT_MAX_TIMEOUT_MS;
        long initialTimeoutMs = (config != null && config.getInitialTimeoutMs() > 0)
                ? config.getInitialTimeoutMs()
                : DEFAULT_INITIAL_TIMEOUT_MS;

        long startNano = System.nanoTime();
        int attempt = 0;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                attempt++;
                long elapsedMs = elapsedMillis(startNano);

                if (elapsedMs >= maxTimeoutMs) {
                    log.error("[{}] Resilience budget exhausted ({} ms / {} ms). Aborting job.",
                            workerName, elapsedMs, maxTimeoutMs);
                    break;
                }

                try {
                    log.info("[{}] Starting attempt {} (elapsed: {} ms / budget: {} ms)...",
                            workerName, attempt, elapsedMs, maxTimeoutMs);

                    recordsProcessed = doExecute();
                    exitCode = 0;
                    log.info("[{}] Completed successfully on attempt {}. Records processed: {}.",
                            workerName, attempt, recordsProcessed);
                    break;

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] Execution interrupted during attempt {}.", workerName, attempt);
                    break;

                } catch (Exception ex) {
                    elapsedMs = elapsedMillis(startNano);
                    long remainingBudgetMs = maxTimeoutMs - elapsedMs;

                    log.error("[{}] Synchronization error on attempt {} (remaining budget: {} ms).",
                            workerName, attempt, Math.max(0, remainingBudgetMs), ex);

                    if (remainingBudgetMs <= 0) {
                        log.error("[{}] No time budget left after attempt {}. Aborting.", workerName, attempt);
                        break;
                    }

                    long delayMs = calculateBackoffWithJitter(initialTimeoutMs, attempt, maxTimeoutMs);

                    /* If the delay consumes the entire remaining budget, there’s no point in waiting — 
                     * the attempt won’t have time to start anyway.
                     */
                    if (delayMs >= remainingBudgetMs) {
                        log.warn("[{}] Calculated backoff ({} ms) exceeds remaining budget ({} ms). Aborting retries.",
                                workerName, delayMs, remainingBudgetMs);
                        break;
                    }

                    log.info("[{}] Backoff with jitter: sleeping {} ms before next attempt...", workerName, delayMs);
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Backoff sleep interrupted. Terminating sequence.", workerName);
            exitCode = 1;
        } finally {
            try {
                ExecutionResultWriter.writeWorkerResult(workerName, exitCode, recordsProcessed);
            } catch (Exception writeEx) {
                log.error("[{}] Failed to write worker execution result.", workerName, writeEx);
            } finally {
                running.set(false);
                log.info("[{}] Pipeline lock released.", workerName);
            }
        }
    }

    /**
     * Safe calculation of exponential backoff with jitter without long overflow risks.
     */
    private long calculateBackoffWithJitter(long initialIntervalMs, int attempt, long maxCapMs) {
        /* Limit the exponent from above by the value of maxCapMs to avoid overflow of the long type 
         * when the number of attempts is large.
         */
        double rawInterval = initialIntervalMs * Math.pow(MULTIPLIER, Math.min(attempt - 1, 30));
        long cappedInterval = (long) Math.min(rawInterval, maxCapMs);

        long delta = (long) (cappedInterval * RANDOMIZATION_FACTOR);
        long minDelay = Math.max(0L, cappedInterval - delta);
        long maxDelay = Math.max(minDelay, cappedInterval + delta);

        return ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1);
    }

    private long elapsedMillis(long startNano) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    }

    public abstract String getWorkerName();

    protected abstract Logger getLogger();

    public abstract ApiConfig.WorkerConfig getWorkerConfig();

    protected abstract int doExecute() throws Exception;
}
