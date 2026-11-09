package by.beloil.shared.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Infrastructure path utility responsible for dynamic, cross-platform location 
 * of the core configuration directory and internal profile environment assets.
 */
public class ConfigPathResolver {

    private ConfigPathResolver() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Resolves the target configuration folder path relative to the active execution location
     * both when running inside an integrated IDE workspace or from an external compiled JAR asset.
     * 
     * @param directoryName the plain name of the targeted configuration folder (e.g., "config")
     * @return a validated File handler pointing directly to the configuration folder root
     * @throws FileNotFoundException if the configuration boundary folder layout cannot be found
     */
    public static File locate(String directoryName) throws FileNotFoundException {
        // Strategy 1: Attempt to locate the directory based on the physical location of this class or compiled JAR
        try {
            String encodedPath = ConfigPathResolver.class.getProtectionDomain()
                    .getCodeSource().getLocation().getPath();
            String decodedPath = URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
            
            File executionLocation = new File(decodedPath);
            // If running inside a JAR, executionLocation points to the JAR file. 
            // We climb up to its parent folder to look for the sibling directoryName folder.
            File parentDir = executionLocation.isDirectory() ? executionLocation : executionLocation.getParentFile();
            
            // In a standard multi-module layout, if target/ is present, step out to project root
            if (parentDir != null && ("target".equalsIgnoreCase(parentDir.getName()) || parentDir.getAbsolutePath().contains("target"))) {
                while (parentDir != null && !new File(parentDir, directoryName).exists()) {
                    parentDir = parentDir.getParentFile();
                }
            }

            if (parentDir != null) {
                File resolvedDir = new File(parentDir, directoryName);
                if (resolvedDir.exists() && resolvedDir.isDirectory()) {
                    return resolvedDir;
                }
            }
        } catch (Exception ignored) {
            // Fallback immediately to standard environment context rules on fail
        }

        // Strategy 2: Fallback to the current system runtime working execution user directory layout context
        File currentWorkingDir = new File(System.getProperty("user.dir"));
        File resolvedDir = new File(currentWorkingDir, directoryName);
        
        if (resolvedDir.exists() && resolvedDir.isDirectory()) {
            return resolvedDir;
        }

        // Strategy 3: Deep check - crawl up to find sibling if executed from subfolder contexts
        File checkDir = currentWorkingDir;
        while (checkDir != null) {
            File potentialDir = new File(checkDir, directoryName);
            if (potentialDir.exists() && potentialDir.isDirectory()) {
                return potentialDir;
            }
            checkDir = checkDir.getParentFile();
        }

        // If all dynamic cross-platform strategies fail, throw a verbose deployment exception
        throw new FileNotFoundException("Critical configuration directory '" + directoryName 
                + "' is missing from the environment path setup. The application engine cannot start without a valid external orchestration baseline folder.");
    }

    /**
     * Locates a explicit configuration property file nested inside the centralized orchestration directory.
     * 
     * @param fileName plain file layout token mapping identifier (e.g., "application.yaml")
     * @return a direct validated File location boundary instance reference
     * @throws FileNotFoundException if the configuration text profile layout is corrupted or missing on disk
     */
    public static File locateConfigFile(String fileName) throws FileNotFoundException {
        File configDirectory = locate("config");
        File targetFile = new File(configDirectory, fileName);
        
        if (!targetFile.exists() || !targetFile.isFile()) {
            throw new FileNotFoundException("Required active environmental profile mapping target configuration file is missing: " 
                    + targetFile.getAbsolutePath());
        }
        return targetFile;
    }
}