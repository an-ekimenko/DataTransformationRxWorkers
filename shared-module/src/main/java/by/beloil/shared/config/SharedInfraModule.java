package by.beloil.shared.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.FileNotFoundException;
import java.io.InputStream;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SharedInfraModule extends AbstractModule {
    
    private static final Logger logger = LogManager.getLogger(SharedInfraModule.class);
    private static final YAMLMapper yamlMapper = new YAMLMapper();

    @Override
    protected void configure() {
    }

    @Provides 
    @Singleton
    public SessionFactory provideMSSQLSessionFactory() throws FileNotFoundException {
        logger.info("Shared Infrastructure: Bootstrapping dynamic master SessionFactory for MS SQL...");
        try {
            java.io.File externalFile = ConfigPathResolver.locateConfigFile("Hibernate.cfg.xml");
            Configuration configuration = new Configuration().configure(externalFile);
            
            // Dynamically read the list of entities from the resources file entities.yaml.
            try (InputStream is = getClass().getClassLoader().getResourceAsStream("entities.yaml")) {
                if (is == null) {
                    logger.warn("Configuration file 'entities.yaml' not found in shared-module classpath resources.");
                } else {
                    JsonNode root = yamlMapper.readTree(is);
                    JsonNode entitiesNode = root.get("entities");
                    
                    if (entitiesNode != null && entitiesNode.isArray()) {
                        for (JsonNode node : entitiesNode) {
                            String entityClassName = node.asText();
                            try {
                                Class<?> entityClass = Class.forName(entityClassName);
                                configuration.addAnnotatedClass(entityClass);
                                logger.info("Successfully bound external entity class from YAML: {}", entityClassName);
                            } catch (ClassNotFoundException e) {
                                logger.warn("Dynamic module entity omitted (not present in active runtime classpath): {}", entityClassName);
                            }
                        }
                    }
                }
            }

            return configuration.buildSessionFactory(
                new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build()
            );
        } catch (Exception e) {
            logger.fatal("Failed to initialize master MS SQL database pool via dynamic context.", e);
            throw new RuntimeException(e);
        }
    }
}
