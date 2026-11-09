package fi;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.name.Named;

import by.beloil.shared.config.ApiConfig;
import by.beloil.shared.repository.StagingBatchRepository;
import fi.model.F12DataFromApi_v3;
import fi.model.F12ItemFromApi_v3;
import fi.model.F12_Entity;
import fi.model.F12_PK;
import fi.model.F12t_Entity;
import fi.network.FI_HttpClient;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

/**
 * Main application orchestrator coordinating the Financial Indicators (Form-12) data pipeline.
 */
@Singleton
public class FI_App {

    private static final Logger logger = LogManager.getLogger(FI_App.class);

    private final FI_HttpClient httpClient;
    private final StagingBatchRepository<F12_Entity> f12Repository;
    private final ObjectMapper objectMapper;
    private final ApiConfig config;
    private final SessionFactory oracleSessionFactory;

    @Inject
    public FI_App(
            FI_HttpClient httpClient, 
            StagingBatchRepository<F12_Entity> f12Repository, 
            @Named("fi") ObjectMapper objectMapper, 
            ApiConfig config,
            @Named("oracle") SessionFactory oracleSessionFactory) {
        this.httpClient = httpClient;
        this.f12Repository = f12Repository;
        this.objectMapper = objectMapper;
        this.config = config;
        this.oracleSessionFactory = oracleSessionFactory;
    }

    /**
     * Executes the complete data synchronization workflow.
     * 
     * @return total number of committed database entity records from both routes
     */
    public int run() {
        logger.info("Starting Parallel Financial Indicators integration pipeline job...");
        
        try {
            // Asynchronous Execution Path 1: Pull KPI telemetry entries from the Oracle Database
            CompletableFuture<List<F12_Entity>> oracleFuture = CompletableFuture.supplyAsync(() -> {
                List<F12t_Entity> arrF12t = getF12tDataFromOracle();
                return arrF12t.stream().map(x -> new F12_Entity(
                        new F12_PK(x.getId().getYYYYMM(), Integer.parseInt(x.getId().getMemberCode()), "01"),
                        0, x.getValue_01t()
                )).collect(Collectors.toList());
            }).exceptionally(ex -> {
                logger.error("Asynchronous fault encountered inside the Oracle source reading thread path.", ex);
                return new ArrayList<>();
            });

            // Asynchronous Execution Path 2: Pull Form-12 accounting metrics from the external API Gateway
            CompletableFuture<List<F12_Entity>> apiFuture = CompletableFuture.supplyAsync(this::getF12DataFromAPI)
                    .exceptionally(ex -> {
                        logger.error("Asynchronous fault encountered inside the external REST API pulling thread path.", ex);
                        return new ArrayList<>();
                    });

            // Reactive Consolidation: Merge results from both threads upon concurrent completion
            List<F12_Entity> combinedResult = oracleFuture.thenCombineAsync(apiFuture, (s1, s2) -> 
                Stream.concat(s1.stream(), s2.stream()).collect(Collectors.toList())
            ).get();

            if (combinedResult.isEmpty()) {
                logger.warn("Aggregated datasets are completely empty. Database commit transaction bypassed.");
                return 0; // Return zero records processed
            }

            logger.info("Parallel extraction task completed. Total combined metrics records processed: {}", combinedResult.size());
            if (logger.isDebugEnabled()) {
                combinedResult.forEach(x -> logger.debug("Mapped Indicator Metadata: Key[{}], Member[{}], Value[{}], Status[{}]", 
                        x.getId().getCode(), x.getId().getMemberCode(), x.getValue(), x.getStatus()));
            }

            // Persistence Flush: Save integrated records using high-performance batching
            logger.info("Flushing {} integrated F12 financial metrics records to MS SQL database staging...", combinedResult.size());
            f12Repository.saveOrUpdateBatch(combinedResult);
            
            logger.info("Financial Indicators synchronization pipeline completed successfully!");
            
            return combinedResult.size(); // Return actual processed batch size
        } catch (Exception e) {
            logger.error("Critical failure encountered inside F12 data processing orchestration boundary.", e);
            throw new RuntimeException("F12 synchronization task failed", e);
        }
    }

    private List<F12t_Entity> getF12tDataFromOracle() {
        logger.info("Executing transactional query path to capture KPI analytics records from Oracle repository...");
        try (Session session = oracleSessionFactory.openSession()) {
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<F12t_Entity> cr = cb.createQuery(F12t_Entity.class);
            Root<F12t_Entity> root = cr.from(F12t_Entity.class);
            cr.select(root);
            return session.createQuery(cr).getResultList();
        } catch (Exception e) {
            logger.error("Database connection fault occurred while streaming transaction metadata from Oracle.", e);
            throw e;
        }
    }

    private List<F12_Entity> getF12DataFromAPI() {
        logger.info("Executing algorithmic calculation script for Financial Indicators scheduling period...");
        ZoneId z = ZoneId.of("Europe/Minsk");
        LocalDate today = LocalDate.now(z);
        
        int day = today.getDayOfMonth();
        int month = today.getMonthValue();
        int year = today.getYear();
        ArrayList<String> arrMonths = new ArrayList<>();

        logger.debug("Current calendar context evaluation: Date [{}], Day [{}], Month [{}]", today, day, month);

        if (month == 1) {
            arrMonths.add(day > 24 ? "12" : "11"); 
            year -= 1;
        } else if (month == 2) {
            if (day > 24) { 
                arrMonths.add("01"); 
            } else { 
                arrMonths.add("12"); 
                year -= 1; 
            }
        } else {
            arrMonths.add(String.format("%02d", day > 24 ? month - 1 : month - 2));
        }

        logger.info("Calculated target year context: [{}], Target extraction months mapping: {}", year, arrMonths);

        List<F12_Entity> result = new ArrayList<>();
        for (String m : arrMonths) {
            try {
                result.addAll(processSingleURL(year, m));
            } catch (Exception e) {
                logger.error("Failed pulling target REST API data for interval: {}-{}", year, m, e);
            }
        }
        return result;
    }

    private List<F12_Entity> processSingleURL(int year, String month) throws Exception {
        ApiConfig.WorkerConfig fiConfig = config.getFiWorker();
        if (fiConfig == null || fiConfig.getDataEndpoint() == null || fiConfig.getDataEndpoint().isBlank()) {
            throw new IllegalStateException("API data endpoint URI for Financial Indicators is missing in configuration files.");
        }

        String targetUrl = fiConfig.getDataEndpoint() + "?year=" + year + "&month=" + month;
        logger.info("Preparing network data transfer channel context. Destination URL: {}", targetUrl);
        
        String json = httpClient.fetchRawJson(targetUrl);
        if (json == null || json.isBlank()) {
            throw new IllegalStateException("Received completely empty or corrupted payload from Financial indicators API endpoint.");
        }

        F12DataFromApi_v3 parsedData = objectMapper.readValue(json, F12DataFromApi_v3.class);
        List<F12_Entity> mappedEntities = new ArrayList<>();

        if (parsedData != null && parsedData.getF12_list() != null) {
            for (F12ItemFromApi_v3 item : parsedData.getF12_list()) {
                String yyyymm = item.getYear() + String.format("%02d", item.getMonth());
                int memberCode = Integer.parseInt(item.getCodeEmitent());
                
                if (item.getData() != null && item.getData().getAttributes() != null) {
                    item.getData().getAttributes().forEach((code, value) -> mappedEntities.add(new F12_Entity(
                            new F12_PK(yyyymm, memberCode, code), item.getStatus(), value
                    )));
                }
            }
        }
        
        logger.info("REST API processing batch completed: generated {} uncommitted F12 entities.", mappedEntities.size());
        return mappedEntities;
    }
}
