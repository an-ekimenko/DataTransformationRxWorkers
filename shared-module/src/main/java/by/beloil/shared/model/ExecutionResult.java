package by.beloil.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Data Transfer Object (DTO) used to serialize detailed multi-worker execution metrics.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonTypeName("output")
@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
public class ExecutionResult {

    @JsonProperty("datetime")
    private String datetime;

    @JsonProperty("workers")
    private Map<String, WorkerMetrics> workers = new LinkedHashMap<>();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkerMetrics {
        @JsonProperty("returned_code")
        private int returnedCode;

        @JsonProperty("records_processed")
        private int recordsProcessed;
    }
}
