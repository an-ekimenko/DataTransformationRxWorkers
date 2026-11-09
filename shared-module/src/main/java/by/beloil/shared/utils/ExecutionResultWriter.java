package by.beloil.shared.utils;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ConfigPathResolver;
import by.beloil.shared.model.ExecutionResult;

/**
 * Shared utility component responsible for atomically merging worker processing state metrics 
 * into the centralized 'exchange/result.yaml' file.
 */
public class ExecutionResultWriter {

    private static final Logger logger = LogManager.getLogger(ExecutionResultWriter.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd:HH:mm:ss");
    private static final YAMLMapper yamlMapper = new YAMLMapper();

    private ExecutionResultWriter() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Atomically merges or writes individual worker performance metrics into the centralized YAML report.
     */
    public static synchronized void writeWorkerResult(String workerName, int returnedCode, int recordsProcessed) {
        try {
            File rootDir = ConfigPathResolver.locate("config").getParentFile();
            File exchangeDir = new File(rootDir, "exchange");
            
            if (!exchangeDir.exists()) {
                exchangeDir.mkdirs();
            }

            File resultFile = new File(exchangeDir, "result.yaml");
            ExecutionResult report;

            // Strategy: Read existing report file to preserve concurrent metrics from other active workers
            if (resultFile.exists() && resultFile.length() > 0) {
                try {
                    report = yamlMapper.readValue(resultFile, ExecutionResult.class);
                    if (report.getWorkers() == null) {
                        report.setWorkers(new LinkedHashMap<>());
                    }
                } catch (Exception parseException) {
                    logger.warn("Corrupted or older result matrix structure detected. Initializing clean sequence.");
                    report = new ExecutionResult(LocalDateTime.now().format(FORMATTER), new LinkedHashMap<>());
                }
            } else {
                report = new ExecutionResult(LocalDateTime.now().format(FORMATTER), new LinkedHashMap<>());
            }

            // Update execution timestamp and embed worker specific metrics nodes
            report.setDatetime(LocalDateTime.now().format(FORMATTER));
            report.getWorkers().put(workerName, new ExecutionResult.WorkerMetrics(returnedCode, recordsProcessed));

            // Commit the updated structural text map back to the yaml file
            yamlMapper.writeValue(resultFile, report);
            logger.info("Execution state transaction successfully updated for [{}]: {}", workerName, resultFile.getAbsolutePath());
        } catch (Exception e) {
            logger.error("Catastrophic environment fault: failed to write worker execution metrics into exchange directory.", e);
        }
    }
}
