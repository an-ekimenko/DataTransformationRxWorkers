package by.beloil.shared.config;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Infrastructure utility component responsible for loading and parsing external 
 * application configurations formatted as standard YAML profiles.
 */
public class YAMLConfigLoader {
    
    private static final Logger logger = LogManager.getLogger(YAMLConfigLoader.class);
    private static final YAMLMapper mapper = new YAMLMapper();

    private YAMLConfigLoader() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Locates the requested YAML configuration file within the deployment directory structure,
     * parses its content, and maps it directly into the unified application configuration context.
     * 
     * @param fileName the plain name of the file inside the configuration directory (e.g., "application.yaml")
     * @return the fully instantiated and mapped AppApiConfig configuration object tree
     * @throws RuntimeException if a fatal IO error occurs or if the configuration profile layout is corrupted
     */
    public static ApiConfig load(String fileName) {
        try {
            // Dynamically locate the configuration file inside the root config directory layout context
            File configFile = ConfigPathResolver.locateConfigFile(fileName);
            logger.info("Loading application structural environment deployment map from target path: {}", 
                    configFile.getAbsolutePath());
            
            // Map the parsed YAML properties directly into the corporate configuration node tree
            return mapper.readValue(configFile, ApiConfig.class);
        } catch (Exception e) {
            logger.fatal("Fatal deployment exception: failed to load or parse global application configuration properties.", e);
            throw new RuntimeException("Staging context synchronization engine initialization failure", e);
        }
    }
}