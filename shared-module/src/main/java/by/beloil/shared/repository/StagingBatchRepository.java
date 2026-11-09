package by.beloil.shared.repository;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * High-performance generic batch repository responsible for data persistence 
 * across corporate staging database environments.
 *
 * @param <T> the persistent entity type managed by this repository layer
 */
@Singleton
public class StagingBatchRepository<T> {

    private static final Logger logger = LogManager.getLogger(StagingBatchRepository.class);
    private final SessionFactory sessionFactory;

    /**
     * Constructs a new generic staging repository instance with injected dependencies.
     * 
     * @param sessionFactory the centralized Hibernate SessionFactory reference
     */
    @Inject
    public StagingBatchRepository(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Synchronizes a collection of entity models with the target database staging tables 
     * using memory-efficient transaction batching. Automatically handles periodic cache 
     * flushes to optimize memory allocation thresholds.
     *
     * @param entities the list of domain entities to be merged into the persistent context
     */
    public void saveOrUpdateBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            logger.debug("Execution bypassed: target entity data payload is empty.");
            return;
        }

        // Dynamically resolve entity class name for cleaner structured logging outputs
        String entityClassName = entities.get(0).getClass().getSimpleName();
        Transaction transaction = null;

        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();

            int counter = 0;
            for (T entity : entities) {
                // Using merge operation to handle both inserts and updates safely
                session.merge(entity);

                // Periodic cache flushing pattern to optimize memory allocation thresholds
                if (++counter % 50 == 0) {
                    session.flush();
                    session.clear();
                }
            }

            transaction.commit();
            logger.info("Batch operation successful: committed {} records to the {} database schema table context.", 
                    entities.size(), entityClassName);
        } catch (Exception ex) {
            if (transaction != null && transaction.isActive()) {
                transaction.rollback();
            }
            logger.error("Catastrophic boundary fault: package transfer failed while flushing batch metadata for entity: {}", 
                    entityClassName, ex);
            throw ex;
        }
    }
}