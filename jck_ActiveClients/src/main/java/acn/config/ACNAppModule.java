package acn.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.config.YAMLConfigLoader;
import by.beloil.shared.repository.StagingBatchRepository;
import acn.model.ACN_Entity;
import acn.model.ACN_Data;
import acn.jackson.ACN_DataDeserializer;

/**
 * Guice dependency injection module responsible for configuring core infrastructure,
 * object mapping rules, and registering local domain entities mapping handlers.
 */
public class ACNAppModule extends AbstractModule {
	
	private static final Logger logger = LogManager.getLogger(ACNAppModule.class);

	@Override
	protected void configure() {
		// Bind generic staging repository explicitly typed for the local ACN database model
		bind(new TypeLiteral<StagingBatchRepository<ACN_Entity>>(){});
	}

	@Provides 
	@Singleton
	public ApiConfig provideConfig() {
		return YAMLConfigLoader.load("application.yaml");
	}
	
	@Provides 
	@Singleton
	@Named("acn")
	public ObjectMapper provideObjectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		SimpleModule module = new SimpleModule();
		module.addDeserializer(ACN_Data.class, new ACN_DataDeserializer());
		mapper.registerModule(module);
		return mapper;
	}

	@Provides 
	@Singleton
	public HttpClientBuilder provideHttpClientBuilder(ApiConfig config) {
		long timeoutMs = (config.getAcnWorker() != null) ? config.getAcnWorker().getInitialTimeoutMs() : 10000;
		if (timeoutMs < 700000) {
			timeoutMs = 700000; // Safeguard threshold for heavy enterprise corporate gateways
		}
		
		logger.info("Configuring base Apache HTTP Client Builder with global execution timeouts: {} ms", timeoutMs);
		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(Timeout.ofMilliseconds(timeoutMs))
				.setResponseTimeout(Timeout.ofMilliseconds(timeoutMs))
				.build();
		return HttpClients.custom().setDefaultRequestConfig(requestConfig);
	}
}
