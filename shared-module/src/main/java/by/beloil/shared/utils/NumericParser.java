package by.beloil.shared.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared utility component responsible for defensive and fault-tolerant parsing 
 * of numeric string tokens across various enterprise data integration pipelines.
 */
public final class NumericParser {

    private static final Logger logger = LogManager.getLogger(NumericParser.class);

    private NumericParser() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Safely parses a raw string input into a primitive integer.
     * Normalizes whitespace characters and truncates floating-point fractional blocks 
     * to safeguard the extraction pipeline from numeric format mismatches.
     * 
     * @param rawValue the incoming string token to be evaluated and translated
     * @return the parsed integer value, or 0 if a formatting breakdown occurs
     */
    public static int parseSafeInteger(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return 0;
        }
        try {
            // Remove all inline whitespace segments and boundary padding
            String sanitized = rawValue.replaceAll("\\s+", "");
            
            // If a floating-point token is captured accidentally, isolate the integer head component
            if (sanitized.contains(",") || sanitized.contains(".")) {
                sanitized = sanitized.split("[.,]")[0];
            }
            return Integer.parseInt(sanitized);
        } catch (NumberFormatException e) {
            logger.error("Critical integrity variance: failed to parse '{}' as integer. Defaulting to 0", rawValue);
            return 0;
        }
    }

    /**
     * Safely parses a raw string input into a primitive double.
     * Standardizes locale-specific fractional indicators by replacing commas with periods
     * and guarantees runtime resilience via default fallback values upon transformation faults.
     * 
     * @param rawValue the incoming string token to be evaluated and translated
     * @return the parsed double value, or 0.0 if a formatting breakdown occurs
     */
    public static double parseSafeDouble(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return 0.0;
        }
        try {
            // Localization normalization: replace fractional commas with periods and purge inner spaces
            String sanitized = rawValue.replace(",", ".").replaceAll("\\s+", "");
            return Double.parseDouble(sanitized);
        } catch (NumberFormatException e) {
            logger.warn("Payload numeric variance caught: unable to parse '{}' as double. Falling back to 0.0", rawValue);
            return 0.0;
        }
    }
}
