package acn;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.name.Named;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.repository.StagingBatchRepository;
import acn.model.ACN;
import acn.model.ACN_Data;
import acn.model.ACN_Entity;
import acn.model.ACN_PK;
import acn.network.ACN_HttpClient;

/**
 * Main orchestration component responsible for executing the business logic workflow 
 * of the Active Clients Number (ACN) data integration pipeline.
 */
@Singleton
public class ACN_App {

	private static final Logger logger = LogManager.getLogger(ACN_App.class);

	private final ACN_HttpClient httpClient;
	private final StagingBatchRepository<ACN_Entity> stagingRepository;
	private final ObjectMapper objectMapper;
	private final ApiConfig config;

	@Inject
	public ACN_App(
			ACN_HttpClient httpClient, 
			StagingBatchRepository<ACN_Entity> stagingRepository, 
			@Named("acn") ObjectMapper objectMapper, 
			ApiConfig config) {
		this.httpClient = httpClient;
		this.stagingRepository = stagingRepository;
		this.objectMapper = objectMapper;
		this.config = config;
	}

	/**
	 * Executes the data transformation pipeline.
	 * 
	 * @return total number of committed database entity records
	 */
	public int run() {
		logger.info("Initiating Active Clients telemetry integration data pipeline workflow...");
		
		ApiConfig.WorkerConfig workerConfig = config.getAcnWorker();
		if (workerConfig == null || workerConfig.getDataEndpoint() == null || workerConfig.getDataEndpoint().isBlank()) {
			throw new IllegalStateException("API data endpoint URI target configuration is missing for ACN module.");
		}

		// Calculate start and end dates of the previous month based on the Minsk time zone
		ZoneId z = ZoneId.of("Europe/Minsk");
		LocalDate today = LocalDate.now(z);
		LocalDate prev = today.plusMonths(-1);
		String prev_from = prev.toString().substring(0, 7) + "-" + "01";
		String prev_to = prev.toString().substring(0, 7) + "-" + prev.lengthOfMonth();

		// Construct final API endpoint URL using data_endpoint field
		String targetUrl = workerConfig.getDataEndpoint() + "?periodbegin="
		        + prev_from + "&" + "periodend=" + prev_to;

		logger.info("Target connection channel context resolved. Destination URI: {}", targetUrl);

		try {
			// Pull raw telemetry payload from target gateway
			String jsonResponse = httpClient.fetchRawJson(targetUrl);
			if (jsonResponse == null || jsonResponse.isBlank()) {
				logger.warn("Pipeline execution aborted: received empty response string.");
				return 0; // Return zero records processed
			}

			// Deserialize JSON payload into root mapping models
			ACN_Data parsedPayload = objectMapper.readValue(jsonResponse, ACN_Data.class);
			if (parsedPayload == null || parsedPayload.getAcnList() == null || parsedPayload.getAcnList().isEmpty()) {
				logger.warn("Telemetry stream check warning: translated JSON structure is empty.");
				return 0; // Return zero records processed
			}

			// Map transaction payload entities into database entities matrix
			int calendarPeriod = Integer.parseInt(parsedPayload.getYYYYMM());
			List<ACN_Entity> databaseEntitiesBatch = new ArrayList<>();

			logger.info("Transforming transient payload entries for reporting interval period: {}", calendarPeriod);
			for (ACN telemetryRow : parsedPayload.getAcnList()) {
				ACN_PK primaryKey = new ACN_PK(calendarPeriod, telemetryRow.getMemberCode());
				ACN_Entity persistenceModel = new ACN_Entity(
						primaryKey,
						telemetryRow.getActiveClients_Month(),
						telemetryRow.getActiveClients_Year()
				);
				databaseEntitiesBatch.add(persistenceModel);
			}

			// Bulk write entities batch into database persistence storage
			logger.info("Streaming {} synchronized active client data models into database persistence storage...", databaseEntitiesBatch.size());
			stagingRepository.saveOrUpdateBatch(databaseEntitiesBatch);
			
			logger.info("Active Clients volume tracking data integration job finalized successfully.");
			
			return databaseEntitiesBatch.size(); // Return actual batch size upon successful commit
			
		} catch (IOException e) {
			logger.error("Data binding or format reflection error encountered during parsing transaction sequence stream.", e);
			throw new RuntimeException("Failing node mapping transformation layer blockade", e);
		} catch (Exception e) {
			logger.error("Catastrophic network channel breakdown or unhandled boundary thread exception caught inside ACN runtime block.", e);
			throw new RuntimeException("ACN integration run failed", e);
		}
	}
}
