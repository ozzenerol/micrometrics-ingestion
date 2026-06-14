package io.micrometrics.ingestion.database;

import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometrics.ingestion.config.ConfigProvider;
import io.micrometrics.ingestion.config.model.DatabaseModel;

import java.net.InetSocketAddress;

public enum ConnectionProvider {

    INSTANCE;

    private final CqlSession session;

    ConnectionProvider() {
        DatabaseModel database = ConfigProvider.INSTANCE.config().database();
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(database.host(), database.port()))
                .withLocalDatacenter(database.localDataCenter())
                .withAuthCredentials(database.username(), database.password())
                .withKeyspace(database.keyspace())
                .build();
    }

    public CqlSession session() {
        return session;
    }
}
