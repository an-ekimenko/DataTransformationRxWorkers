package rx;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.google.inject.Guice;
import com.google.inject.Injector;

import acn.config.ACNAppModule;
import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.config.ConfigPathResolver;
import by.beloil.shared.config.SharedInfraModule;
import by.beloil.shared.worker.AbstractResilientWorker;
import fi.config.FIAppModule;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;

/**
 * Main orchestration entry point for RxDataTransformationWorkers.
 * Manages application lifecycle, dependency injection, and reactive scheduling for worker tasks.
 * Supports an emergency immediate execution mode via the "xn" flag, optionally scoped to a single worker alias.
 */
public class RxSchedulerMain {

    private static final String EMERGENCY_FLAG = "xn";
    private static final List<String> WORKER_PACKAGES = List.of("acn", "fi");

    private static Logger logger;

    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Map<String, LocalDateTime> lastExecutionMap = new ConcurrentHashMap<>();
    private final LaunchOptions options;

    public RxSchedulerMain(LaunchOptions options) {
        this.options = options;
    }

    public static void main(String[] args) {
        configureLoggingContext();
        logger = LogManager.getLogger(RxSchedulerMain.class);

        LaunchOptions options;
        try {
            options = LaunchOptions.parse(args);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid command line arguments: {}", e.getMessage());
            LogManager.shutdown();
            System.exit(1);
            return;
        }

        RxSchedulerMain app = new RxSchedulerMain(options);
        try {
            app.start();
        } catch (InterruptedException e) {
            logger.warn("Main execution thread interrupted. Initiating shutdown.");
            Thread.currentThread().interrupt();
            app.shutdown(0);
        } catch (Exception e) {
            logger.fatal("Fatal breakdown during central scheduler startup sequence.", e);
            app.shutdown(1);
        }
    }

    public void start() throws InterruptedException {
        if (options.isEmergencyMode()) {
            String target = options.getTargetWorker();
            String targetInfo = (target != null)
                    ? "TARGET WORKER ALIAS ONLY: [" + target + "]"
                    : "ALL CONFIGURED WORKERS";
            logger.warn("CRITICAL EMERGENCY MODE ACTIVE [{}]. Bypassing schedules ({}).", EMERGENCY_FLAG, targetInfo);
        }
        logger.info("Starting Multi-Module Reactive Engine [RxDataTransformationWorkers]...");

        Injector injector = Guice.createInjector(
                new SharedInfraModule(),
                new ACNAppModule(),
                new FIAppModule()
        );

        ApiConfig globalConfig = injector.getInstance(ApiConfig.class);
        List<AbstractResilientWorker> activeWorkers = resolveWorkers(injector, globalConfig);

        if (activeWorkers.isEmpty()) {
            logger.error("No active workers resolved for execution (check application.yaml or CLI parameters). Shutting down.");
            shutdown(1);
            return;
        }

        CountDownLatch emergencyLatch = options.isEmergencyMode()
                ? new CountDownLatch(activeWorkers.size())
                : null;

        registerShutdownHook();

        for (AbstractResilientWorker worker : activeWorkers) {
            scheduleWorker(worker, emergencyLatch);
        }

        if (options.isEmergencyMode()) {
            logger.info("Emergency execution triggered. Waiting for {} worker(s) to finish...", activeWorkers.size());
            emergencyLatch.await();
            logger.info("All emergency worker cycles completed.");
            shutdown(0);
        } else {
            logger.info("Scheduling streams active. Main thread entering sleep barrier...");
            while (!disposables.isDisposed()) {
                Thread.sleep(1000);
            }
        }
    }

    /**
     * Resolves active workers dynamically by matching the 'name' class attribute in application.yaml
     * and optional CLI alias targeting.
     */
    private List<AbstractResilientWorker> resolveWorkers(Injector injector, ApiConfig globalConfig) {
        List<ApiConfig.WorkerConfig> configs = globalConfig.getWorkers();
        if (configs == null || configs.isEmpty()) {
            logger.error("No workers defined in 'workers' section of application.yaml.");
            return Collections.emptyList();
        }

        String targetAlias = options.getTargetWorker();
        List<AbstractResilientWorker> resolved = new ArrayList<>();
        boolean aliasFound = false;

        for (ApiConfig.WorkerConfig cfg : configs) {
            String className = (cfg.getName() != null) ? cfg.getName().trim() : null;
            if (className == null || className.isEmpty()) {
                logger.warn("Skipping worker entry with missing 'name' in application.yaml.");
                continue;
            }

            if (targetAlias != null) {
                String alias = (cfg.getAlias() != null) ? cfg.getAlias().trim().toLowerCase() : "";
                if (!targetAlias.equals(alias)) {
                    continue;
                }
                aliasFound = true;
            }

            Class<? extends AbstractResilientWorker> workerClass = loadWorkerClass(className);
            if (workerClass != null) {
                resolved.add(injector.getInstance(workerClass));
            }
        }

        if (targetAlias != null && !aliasFound) {
            logger.error("Requested CLI worker alias '{}' did not match any configured worker in application.yaml.", targetAlias);
        }

        return resolved;
    }

    /**
     * Resolves a worker Class object by its simple class name (e.g., "ACN_Worker") or fully-qualified class name.
     */
    private Class<? extends AbstractResilientWorker> loadWorkerClass(String name) {
        List<String> candidates = new ArrayList<>();
        if (name.contains(".")) {
            candidates.add(name);
        } else {
            for (String pkg : WORKER_PACKAGES) {
                candidates.add(pkg + "." + name);
            }
        }

        for (String fqcn : candidates) {
            try {
                Class<?> clazz = Class.forName(fqcn);
                if (AbstractResilientWorker.class.isAssignableFrom(clazz)) {
                    return clazz.asSubclass(AbstractResilientWorker.class);
                }
                logger.error("Class '{}' does not extend AbstractResilientWorker.", fqcn);
                return null;
            } catch (ClassNotFoundException ignored) {
                // Try next candidate package
            }
        }

        logger.error("Could not find worker class '{}' in configured packages {}.", name, WORKER_PACKAGES);
        return null;
    }

    /**
     * Attaches either a single-shot immediate execution task (emergency mode) or a recurring reactive schedule loop.
     */
    private void scheduleWorker(AbstractResilientWorker worker, CountDownLatch emergencyLatch) {
        String workerName = worker.getWorkerName();

        if (options.isEmergencyMode()) {
            Disposable task = Schedulers.io().scheduleDirect(() -> {
                try {
                    worker.executeTask();
                } catch (Exception e) {
                    logger.error("Emergency execution failed for worker [{}].", workerName, e);
                } finally {
                    if (emergencyLatch != null) {
                        emergencyLatch.countDown();
                    }
                }
            });
            disposables.add(task);
            logger.info("Emergency single-shot task scheduled for [{}].", workerName);
            return;
        }

        ApiConfig.WorkerConfig config = worker.getWorkerConfig();
        if (config == null) {
            logger.error("Worker [{}] configuration could not be resolved. Skipping schedule.", workerName);
            return;
        }

        List<LocalTime> parsedTimes = parseScheduleTimes(config.getStartTime(), workerName);
        if (parsedTimes.isEmpty()) {
            logger.warn("Worker [{}] has no valid start times configured. Skipping schedule.", workerName);
            return;
        }

        Disposable loop = Observable.interval(1, TimeUnit.SECONDS, Schedulers.io())
                .filter(tick -> !worker.isRunning())
                .filter(tick -> shouldExecuteNow(workerName, config.getDays(), parsedTimes))
                .subscribe(
                        tick -> {
                            try {
                                worker.executeTask();
                            } catch (Exception e) {
                                logger.error("Unhandled exception during [{}] execution.", workerName, e);
                            }
                        },
                        err -> logger.error("Fatal reactive stream error in channel [{}].", workerName, err)
                );

        disposables.add(loop);
        logger.info("Reactive schedule attached to [{}].", workerName);
    }

    /**
    * Evaluates whether the current timestamp falls within the 2-second trigger window of any configured schedule slot,
    * while preventing duplicate executions within the same scheduled slot.
    */
    private boolean shouldExecuteNow(String workerName, List<Integer> targetDays, List<LocalTime> targetTimes) {

        LocalDateTime now = LocalDateTime.now();

    	int currentDayOfWeek = now.getDayOfWeek().getValue();
    	if (targetDays != null && !targetDays.isEmpty()
    			&& !targetDays.contains(0) && !targetDays.contains(currentDayOfWeek)) {
    		return false;
    	}

    	LocalTime currentTime = now.toLocalTime();
    	LocalDate today = now.toLocalDate();
    	for (LocalTime targetTime : targetTimes) {
    		long millisDiff = ChronoUnit.MILLIS.between(targetTime, currentTime);
    		// Allow a 0..2000 ms execution window to avoid missed ticks during scheduler jitter
    		if (millisDiff >= 0 && millisDiff < 2000) {
    			LocalDateTime scheduledSlot = LocalDateTime.of(today, targetTime);
    			
    			// The lastExecutionMap stores the scheduled time from the schedule (scheduledSlot).
    			LocalDateTime lastRun = lastExecutionMap.get(workerName);

    			// Prevent double execution on the exact same scheduled slot
    			if (scheduledSlot.equals(lastRun)) {
    				return false;
    			}
    			lastExecutionMap.put(workerName, scheduledSlot);
    			logger.info("Schedule matched for worker [{}] at slot [{}].", workerName, targetTime);
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * Pre-parses string time entries from configuration into LocalTime instances at startup.
     */
    private List<LocalTime> parseScheduleTimes(List<String> rawTimes, String workerName) {

    		if (rawTimes == null || rawTimes.isEmpty()) {
    			return Collections.emptyList();
    		}
    		return rawTimes.stream()
    				.map(timeStr -> {
    					try {
    						return LocalTime.parse(timeStr.trim());
    					} catch (Exception e) {
    						logger.error("Invalid time format '{}' for worker [{}].", timeStr, workerName, e);
    						return null;
    					}
    			})
    			.filter(Objects::nonNull)
    			.collect(Collectors.toList());
    }
    
    /**
    * Registers a JVM shutdown hook to cleanly dispose reactive streams and flush Log4j2 appenders.
    */
    private void registerShutdownHook() {
    	Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    		if (logger != null) {
    			logger.warn("JVM termination signal captured. Disposing active streams...");
    		}
    		
    		disposables.dispose();
    		LogManager.shutdown();
    	}, "rx-scheduler-shutdown-hook"));
    }
    /**
    * Releases all reactive resources, flushes logs, and terminates the JVM with the given exit code.
    */
    private void shutdown(int exitCode) {
    	disposables.dispose();
    	LogManager.shutdown();
    	System.exit(exitCode);
    }
    
    /**
    * Resolves the project directory structure, ensures the 'logs' directory exists,
    * and reconfigures the Log4j2 context dynamically.
    */
    private static void configureLoggingContext() {
    	try {
    		File configDir = ConfigPathResolver.locate("config");
    		File baseDir = configDir.getParentFile();
    		File logsDir = new File(baseDir, "logs");
    		boolean dirCreated = !logsDir.exists() && logsDir.mkdirs();
    		System.setProperty("commonLogFilename", new File(logsDir, "commons.log").getAbsolutePath());
    		System.setProperty("p6spyLogFilename", new File(logsDir, "p6spy.log").getAbsolutePath());
    		LoggerContext context = (LoggerContext) LogManager.getContext(false);
    		File log4jConfig = new File(configDir, "Log4j2.xml");
    		if (log4jConfig.exists()) {
    			context.setConfigLocation(log4jConfig.toURI());
    			context.reconfigure();
    		}
    		if (dirCreated) {
    			LogManager.getLogger(RxSchedulerMain.class)
    				.info("Created missing 'logs' directory at: {}", logsDir.getAbsolutePath());
    		}
    	} catch (Exception e) {
    		System.err.println("Critical error configuring logging architecture: " + e.getMessage());
    		e.printStackTrace();
    	}
    }
    
    /**
    * Encapsulates and validates parsed command-line launch options.
    */
    public static final class LaunchOptions {
    	@Getter private final boolean emergencyMode;
    	@Getter private final String targetWorker;
    	
    	public LaunchOptions(boolean emergencyMode, String targetWorker) {
    		this.emergencyMode = emergencyMode;
    		this.targetWorker = targetWorker;
    	}
    
    	public static LaunchOptions parse(String[] args) {
    		if (args == null || args.length == 0) {
    			return new LaunchOptions(false, null);
    		}
    		boolean isEmergency = false;
    		String workerAlias = null;
    		for (String rawArg : args) {
    			if (rawArg == null || rawArg.isBlank()) {
    				continue;
    			}
    			String arg = rawArg.trim().toLowerCase();
    			if (EMERGENCY_FLAG.equals(arg)) {
    				isEmergency = true;
    			} else {
    				if (workerAlias != null && !workerAlias.equals(arg)) {
    					throw new IllegalArgumentException(
    							"Multiple target worker aliases specified: '" + workerAlias 
    							+ "' and '" + arg + "'. Specify only one.");
    				}
    				workerAlias = arg;
    			}
    		}
    		if (workerAlias != null && !isEmergency) {
    			throw new IllegalArgumentException(
    					"Target worker alias '" + workerAlias + "' can only be specified together with emergency flag '" 
    							+ EMERGENCY_FLAG + "'.");
    		}
    		return new LaunchOptions(isEmergency, workerAlias);
    	}
    }
}