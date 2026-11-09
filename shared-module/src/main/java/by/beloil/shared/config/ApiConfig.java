package by.beloil.shared.config;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Unified application configuration profile for data transformation workers.
 */
@Data
public class ApiConfig {

    @JsonProperty("workers")
    private Map<String, WorkerConfig> workers;

    public WorkerConfig getAcnWorker() {
        return workers != null ? workers.get("acn_worker") : null;
    }

    public WorkerConfig getFiWorker() {
        return workers != null ? workers.get("fi_worker") : null;
    }

    @Data
    public static class WorkerConfig {
        @JsonProperty("data_endpoint")
        private String dataEndpoint; // Target API endpoint for business metrics

        @JsonProperty("auth_type")
        private String authType;     // Auth strategy: "none", "basic", "token", or "oauth2"

        @JsonProperty("auth")
        private AuthConfig auth;

        @JsonProperty("days")
        private List<Integer> days;  // Target schedule days (ISO: 1-7)

        @JsonProperty("start_time")
        private List<String> startTime;

        @JsonProperty("max_attempts")
        private int maxAttempts = 3;

        @JsonProperty("initial_timeout_ms")
        private long initialTimeoutMs = 10000;
    }

    @Data
    public static class AuthConfig {
        @JsonProperty("basic")
        private BasicConfig basic;

        @JsonProperty("static_token")
        private String staticToken; // Pre-generated static API key

        @JsonProperty("oauth2")
        private Oauth2Config oauth2;
    }

    @Data
    public static class BasicConfig {
        private String username;
        private String password;
    }

    @Data
    public static class Oauth2Config {
        @JsonProperty("token_endpoint")
        private String tokenEndpoint; // Auth server URI to request dynamic token
        @JsonProperty("client_id")
        private String clientId;
        @JsonProperty("client_secret")
        private String clientSecret;
        private String scope;
    }
}
