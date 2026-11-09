package fi.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Enterprise analytics DTO wrapper component holding calculated key performance indicators (KPI) 
 * and dynamic financial attributes mapped per corporate issuer.
 */
public class EmitentData_v3 {
  
    /**
     * Internal attribute container holding specific accounting code rows mapped to their computed values.
     * Example entries include: "019/001" -> Return on Sales, "028" -> Net Profit.
     */
    private final HashMap<String, Double> attributes = new HashMap<>();

    /**
     * Default no-arg constructor initializing the underlying financial attribute storage layer.
     */
    public EmitentData_v3() {}

    /**
     * Commits a calculated accounting attribute code and its corresponding monetary value into the local store.
     * 
     * @param code the standardized string accounting code identifier (e.g., "028", "001")
     * @param value the parsed or dynamically aggregated fractional financial metric value
     */
    public void putIfAbsent(String code, double value) {
        attributes.put(code, Double.valueOf(value));
    }

    /**
     * Provides an external read-only or operational view of the underlying financial attributes metadata map.
     * 
     * @return the raw dictionary map holding mapped accounting metrics
     */
    public Map<String, Double> getAttributes() {
        return attributes;
    }
}
