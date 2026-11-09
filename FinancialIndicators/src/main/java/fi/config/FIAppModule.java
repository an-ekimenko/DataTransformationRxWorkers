package fi.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import by.beloil.shared.config.ConfigPathResolver;
import by.beloil.shared.repository.StagingBatchRepository;
import fi.model.F12DataFromApi_v3;
import fi.model.F12_Entity;
import fi.model.F12t_Entity;
import fi.jackson.F12_DataDeserializer_v3;
import java.io.File;
import java.io.FileNotFoundException;

/**
 * Guice dependency injection module responsible for configuring isolated object mapping
 * and managing the dedicated source Oracle database SessionFactory route.
 */
public class FIAppModule extends AbstractModule {

    @Override
    protected void configure() {
        // Bind generic staging repository explicitly typed for the local FI database models
        bind(new TypeLiteral<StagingBatchRepository<F12_Entity>>(){});
    }

    @Provides 
    @Singleton
    @Named("fi")
    public ObjectMapper provideFIObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(F12DataFromApi_v3.class, new F12_DataDeserializer_v3());
        mapper.registerModule(module);
        return mapper;
    }

    @Provides 
    @Singleton
    @Named("oracle")
    public SessionFactory provideOracleSessionFactory() throws FileNotFoundException {
        File externalFile = ConfigPathResolver.locateConfigFile("hibernate_f12.cfg.xml");
        Configuration oracleConfig = new Configuration().configure(externalFile);
        oracleConfig.addAnnotatedClass(F12t_Entity.class);
        return oracleConfig.buildSessionFactory(
            new StandardServiceRegistryBuilder().applySettings(oracleConfig.getProperties()).build()
        );
    }
}
