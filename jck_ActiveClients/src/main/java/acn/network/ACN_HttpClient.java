package acn.network; 

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.message.types.GrantType;

import by.beloil.shared.config.ApiConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import my.CertUpdater;
import my.KeyStoreManager;

/**
 * High-performance HTTP client wrapper dedicated to pulling Active Clients Number telemetry data streams.
 */
@Singleton
public class ACN_HttpClient {
    
    private static final Logger logger = LogManager.getLogger(ACN_HttpClient.class);
    
    private final ApiConfig config;
    private final HttpClientBuilder httpClientBuilder;
    private CloseableHttpClient httpClient;

    @Inject
    public ACN_HttpClient(ApiConfig config, HttpClientBuilder httpClientBuilder) {
        this.config = config;
        this.httpClientBuilder = httpClientBuilder;
    }

    /**
     * Executes a thread-safe HTTP GET request to pull raw JSON telemetry content streams from the destination gateway.
     */
    public String fetchRawJson(String dynamicTargetUrl) throws Exception {
        // Lazy initialization to verify active TLS certificates before first pipeline execution
        synchronized (this) {
            if (this.httpClient == null) {
                if (dynamicTargetUrl.toLowerCase().startsWith("https")) {
                    logger.info("HTTPS protocol scheme detected for ACN gateway channel. Triggering dynamic certificate handshake update hooks...");
                    
                    CertUpdater.updateWithoutAuth(dynamicTargetUrl);
                    
                    SSLContext sslContext = KeyStoreManager.getSSLContext();
                    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
                    
                    PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                            .setSSLSocketFactory(sslSocketFactory)
                            .build();
                            
                    httpClientBuilder.setConnectionManager(connectionManager);
                }
                
                this.httpClient = httpClientBuilder.build();
                logger.info("ACN HTTP transport infrastructure successfully initialized.");
            }
        }

        ApiConfig.WorkerConfig acnConfig = config.getAcnWorker();

        // Create timeout configurations for the long-running ACN pipeline request
        org.apache.hc.client5.http.config.RequestConfig requestConfig = org.apache.hc.client5.http.config.RequestConfig.custom()
                .setConnectTimeout(org.apache.hc.core5.util.Timeout.ofMilliseconds(70000))
                .setResponseTimeout(org.apache.hc.core5.util.Timeout.ofMilliseconds(70000))
                .build();

        ClassicRequestBuilder requestBuilder = ClassicRequestBuilder.get(dynamicTargetUrl)
                .setHeader("Accept", "application/json");

        // Handle routing authorization based on the specified auth_type strategy
        if (acnConfig != null && acnConfig.getAuthType() != null && acnConfig.getAuth() != null) {
            String authType = acnConfig.getAuthType();
            ApiConfig.AuthConfig auth = acnConfig.getAuth();

            if ("basic".equalsIgnoreCase(authType) && auth.getBasic() != null) {
                ApiConfig.BasicConfig basic = auth.getBasic();
                String credentials = basic.getUsername() + ":" + basic.getPassword();
                String encodedAuth = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                requestBuilder.setHeader("Authorization", "Basic " + encodedAuth);
                logger.debug("Applied HTTP Basic authentication headers.");

            } else if ("token".equalsIgnoreCase(authType) && auth.getStaticToken() != null && !auth.getStaticToken().isBlank()) {
                requestBuilder.setHeader("Authorization", "Bearer " + auth.getStaticToken());
                logger.debug("Applied static Bearer API key authentication headers.");

            } else if ("oauth2".equalsIgnoreCase(authType) && auth.getOauth2() != null) {
                ApiConfig.Oauth2Config oauth2 = auth.getOauth2();
                logger.debug("OAuth2 workflow active. Initiating token request to endpoint: {}", oauth2.getTokenEndpoint());

                OAuthClient oauthClient = new OAuthClient(new URLConnectionClient());
                OAuthClientRequest oauthRequest = OAuthClientRequest
                        .tokenLocation(oauth2.getTokenEndpoint())
                        .setGrantType(GrantType.CLIENT_CREDENTIALS)
                        .setClientId(oauth2.getClientId())
                        .setClientSecret(oauth2.getClientSecret())
                        .setScope(oauth2.getScope())
                        .buildBodyMessage();

                String dynamicToken = oauthClient.accessToken(oauthRequest, 
                                                OAuth.HttpMethod.POST, 
                                                OAuthJSONAccessTokenResponse.class)
                                               .getAccessToken();

                requestBuilder.setHeader("Authorization", "Bearer " + dynamicToken);
                logger.debug("Successfully injected dynamic OAuth2 access token into connection headers.");
            }
        }

        ClassicHttpRequest request = requestBuilder.build();
        logger.info("Executing ACN REST API request to target data endpoint locator: {}", dynamicTargetUrl);
        
        // Create and bind the HttpClientContext containing timeout configurations to the execution block
        HttpClientContext context = HttpClientContext.create();
        context.setRequestConfig(requestConfig);

        return httpClient.execute(request, context, response -> {
            int status = response.getCode();
            logger.info("Network transaction snapshot processed. ACN HTTP gateway response status code received: {}", status);

            if (status >= 200 && status < 300) {
                if (response.getEntity() == null) {
                    throw new RuntimeException("ACN API gateway channel exception: server returned a successful status code " 
                            + status + " but an empty response body.");
                }
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            } else {
                throw new RuntimeException("ACN API gateway transaction failure. Remote server returned error status code: " 
                        + status + ". Reason phrase description: " + response.getReasonPhrase());
            }
        });
    }
}
