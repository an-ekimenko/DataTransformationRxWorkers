package rx;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;       
import org.apache.logging.log4j.Logger;           
import org.apache.logging.log4j.core.LoggerContext;

import com.google.inject.Guice;
import com.google.inject.Injector;

import acn.ACN_Worker;
import acn.config.ACNAppModule;
import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.config.ConfigPathResolver;
import fi.config.FIAppModule;
import fi.FI_Worker;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Main orchestration entry point located at the root of RxDataTransformationWorkers.
 * Uses RxJava 3 loop interval cycles to concurrently manage and fire ACN and FI worker tasks.
 * Supports a crisis recovery mode via the "xn" parameter, bypassing schedules while preserving retry policies.
 */
public class RxSchedulerMain {

    private static Logger logger;
    private static final CompositeDisposable disposables = new CompositeDisposable();
    private static boolean immediateExecution = false;
    
    // Coordinates graceful shutdown in emergency run mode only after all retries are exhausted
    private static CountDownLatch emergencyLatch;

    public static void main(String[] args) {
        // Check for emergency immediate execution override argument
        if (args != null && args.length > 0 && "xn".equalsIgnoreCase(args[0])) {
            immediateExecution = true;
        }

        // Initialize systems logging context paths
        configureLoggingContext();
        logger = LogManager.getLogger(RxSchedulerMain.class);
        
        if (immediateExecution) {
            logger.warn("CRITICAL EMERGENCY MODE ACTIVE [xn]. Bypassing cron schedules. Executing workers immediately with active retry policies.");
        }
        logger.info("Starting Multi-Module Reactive Engine Container Room [RxDataTransformationWorkers]...");

        try {
            // Instantiate unified Google Guice Dependency Injection context graph
            Injector injector = Guice.createInjector(
                    new by.beloil.shared.config.SharedInfraModule(),
                    new ACNAppModule(), 
                    new FIAppModule()
            );
            ApiConfig globalConfig = injector.getInstance(ApiConfig.class);

            // Dynamically calculate how many workers are active to set the countdown barrier
            int activeWorkersCount = 0;
            if (globalConfig.getAcnWorker() != null) activeWorkersCount++;
            if (globalConfig.getFiWorker() != null) activeWorkersCount++;
            
            if (immediateExecution) {
                emergencyLatch = new CountDownLatch(activeWorkersCount);
            }

            // ==========================================
            // 1. Active Clients Number Worker Stream Setup
            // ==========================================
            ApiConfig.WorkerConfig acnConfig = globalConfig.getAcnWorker();
            if (acnConfig != null) {
                ACN_Worker acnWorker = injector.getInstance(ACN_Worker.class);
                
                if (immediateExecution) {
                    // Execute exactly once on a background thread without an infinite interval loop
                    Schedulers.io().scheduleDirect(() -> {
                        try {
                            acnWorker.executeTask();
                        } finally {
                            emergencyLatch.countDown();
                        }
                    });
                    logger.info("Emergency execution single-shot task attached to ACN_Worker.");
                } else {
                    // Standard recurring schedule mode
                    disposables.add(Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
                            .filter(tick -> !acnWorker.isRunning())
                            .filter(tick -> shouldExecuteNow(acnConfig, "acn_worker"))
                            .subscribe(
                                    tick -> acnWorker.executeTask(),
                                    err -> logger.error("Fatal exception inside automated ACN execution channel.", err)
                            ));
                    logger.info("Reactive scheduling profile successfully attached to ACN_Worker module loop.");
                }
            }

            // ==========================================
            // 2. Financial Indicators Worker Stream Setup
            // ==========================================
            ApiConfig.WorkerConfig fiConfig = globalConfig.getFiWorker();
            if (fiConfig != null) {
                FI_Worker fiWorker = injector.getInstance(FI_Worker.class);
                
                if (immediateExecution) {
                    // Execute exactly once on a background thread without an infinite interval loop
                    Schedulers.io().scheduleDirect(() -> {
                        try {
                            fiWorker.executeTask();
                        } finally {
                            emergencyLatch.countDown();
                        }
                    });
                    logger.info("Emergency execution single-shot task attached to FI_Worker.");
                } else {
                    // Standard recurring schedule mode
                    disposables.add(Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
                            .filter(tick -> !fiWorker.isRunning())
                            .filter(tick -> shouldExecuteNow(fiConfig, "fi_worker"))
                            .subscribe(
                                    tick -> fiWorker.executeTask(),
                                    err -> logger.error("Fatal exception inside automated FI execution channel.", err)
                            ));
                    logger.info("Reactive scheduling profile successfully attached to FI_Worker module loop.");
                }
            }



            // Attach standard JVM shutdown hooks for resource disposal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.warn("System termination signal captured. Flushing active pipeline streams execution threads.");
                disposables.dispose();
            }));

            if (immediateExecution) {
                logger.info("Emergency execution triggered. Waiting for all integration threads and their retry loops to finish...");
                emergencyLatch.await(); // Safely blocks the main thread until work (and all its retries) is fully complete
                logger.info("All emergency worker cycles and retry policies completed. Shutting down system.");
                disposables.dispose();
                System.exit(0);
            } else {
                logger.info("Multi-Module scheduling streams successfully unrolled. Main thread entering sleep barrier context...");
                while (!disposables.isDisposed()) {
                    Thread.sleep(1000);
                }
            }

        } catch (InterruptedException e) {
            logger.warn("Main execution container environment context thread interrupted. Exiting routine layout.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Fatal breakdown captured during central scheduler core startup execution sequence - " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Determines whether a specific target worker node matches execution criteria based on active schedule profiles.
     */
    private static boolean shouldExecuteNow(ApiConfig.WorkerConfig workerConfig, String workerName) {
        LocalDateTime now = LocalDateTime.now();
        int currentDayOfWeek = now.getDayOfWeek().getValue();
        List<Integer> targetDays = workerConfig.getDays();
        
        if (targetDays != null && !targetDays.isEmpty() && !targetDays.contains(0) && !targetDays.contains(currentDayOfWeek)) {
            return false;
        }

        LocalTime currentTime = now.toLocalTime();
        boolean matched = workerConfig.getStartTime().stream()
                .map(LocalTime::parse)
                .anyMatch(targetTime -> Duration.between(currentTime, targetTime).toSeconds() == 0);

        if (matched) {
            logger.info("Scheduler event notification: time conditions met for target worker component context: [{}].", workerName);
        }
        return matched;
    }

    private static void configureLoggingContext() {
        try {
            // Locates the 'config' folder in the project root
            File configDir = ConfigPathResolver.locate("config");
            File baseDir = configDir.getParentFile(); // Project root directory

            // Fix: Explicitly ensure the physical 'logs' directory exists in the root
            File logsDir = new File(baseDir, "logs");
            if (!logsDir.exists()) {
                boolean created = logsDir.mkdirs();
                if (created) {
                    // System.out.println replaced with logger.info to maintain codebase consistency
                    LogManager.getLogger(RxSchedulerMain.class).info("Infrastructure: Successfully created missing 'logs' directory in the project root.");
                }
            }

            // Bind absolute paths to logging system variables
            System.setProperty("commonLogFilename", new File(logsDir, "commons.log").getAbsolutePath());
            System.setProperty("p6spyLogFilename", new File(logsDir, "p6spy.log").getAbsolutePath());

            // Reconfigure the active Log4j2 core context map
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            File log4jConfig = new File(configDir, "Log4j2.xml");
            
            if (log4jConfig.exists()) {
                context.setConfigLocation(log4jConfig.toURI());
                context.reconfigure();
            }
        } catch (Exception e) {
            System.err.println("Critical pipeline diagnostic error: Logging architecture configuration crash - " + e.getMessage());
        }
    }
}