package io.github.oasisframework.hazelcast.five.helper.connection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import io.github.oasisframework.hazelcast.common.configuration.HazelcastAbstractConfiguration;
import io.github.oasisframework.hazelcast.common.properties.HazelcastConnectionAbstractProperties;
import io.github.oasisframework.hazelcast.five.helper.properties.HazelcastStatusProperties;
import io.github.oasisframework.hazelcast.five.helper.properties.HzHealthChecker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class HazelcastConnection {

    public static final String HAZELCAST_INSTANCE = "hazelcastInstance";
    private static final String MONITORING_LEVEL_KEY = "hazelcast.health.monitoring.level";
    private static final String MONITORING_LEVEL_VALUE = "NOISY";
    private static final String INVOCATION_TIMEOUT_KEY = "hazelcast.client.invocation.timeout.seconds";
    private static final String INVOCATION_TIMEOUT_VALUE = "1";
    private static final int ZERO_TIMEOUT_VALUE = 0;

    private final HazelcastStatusProperties hazelcastStatusProperties;
    private final HazelcastConnectionAbstractProperties hazelcastConnectionProperties;

    @Autowired
    public HazelcastConnection(HazelcastStatusProperties hazelcastStatusProperties, HazelcastConnectionAbstractProperties hazelcastConnectionProperties) {
        this.hazelcastStatusProperties = hazelcastStatusProperties;
        this.hazelcastConnectionProperties = hazelcastConnectionProperties;
    }

    @Bean(HAZELCAST_INSTANCE)
    public HazelcastInstance hazelCastClient() {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(createClientConfig());
        hz.getLifecycleService().addLifecycleListener(this::getHealthCheckListener);
        return hz;
    }

    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();

        if (StringUtils.isBlank(hazelcastConnectionProperties.getConnectionName()) || StringUtils.isBlank(
                hazelcastConnectionProperties.getAddress())) {
            return config;
        }

        config.setNetworkConfig(createClientNetworkConfig());
        config.setConnectionStrategyConfig(createClientConnectionStrategyConfig(config));
       // config.getSerializationConfig().addSerializerConfig(createProtocolBufferSerializer());

        config.setProperty(MONITORING_LEVEL_KEY, MONITORING_LEVEL_VALUE);
        config.setProperty(INVOCATION_TIMEOUT_KEY, INVOCATION_TIMEOUT_VALUE);
        config.setClusterName(hazelcastConnectionProperties.getConnectionName());

        return config;
    }

    private ClientNetworkConfig createClientNetworkConfig() {
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();

        networkConfig.setAddresses(hazelcastConnectionProperties.getAddressList());
        networkConfig.setSmartRouting(true);
        networkConfig.setRedoOperation(true);

        return networkConfig;
    }

    private ClientConnectionStrategyConfig createClientConnectionStrategyConfig(ClientConfig config) {
        ClientConnectionStrategyConfig connectionStrategyConfig = config.getConnectionStrategyConfig();
        connectionStrategyConfig
                .setAsyncStart(false)
                .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ON);

        connectionStrategyConfig.setConnectionRetryConfig(handleConnectionRetryConfig(connectionStrategyConfig.getConnectionRetryConfig()));
        return connectionStrategyConfig;
    }

    private ConnectionRetryConfig handleConnectionRetryConfig(ConnectionRetryConfig connectionRetryConfig) {
        if (hazelcastConnectionProperties.getConnectTimeoutAsMilliSeconds() == null || hazelcastConnectionProperties.getConnectTimeoutAsMilliSeconds() < ZERO_TIMEOUT_VALUE) {
            // equivalent of connection attempt limit is zero
            connectionRetryConfig.setClusterConnectTimeoutMillis(ConnectionRetryConfig.DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS);
        } else {
            connectionRetryConfig.setClusterConnectTimeoutMillis(hazelcastConnectionProperties.getConnectTimeoutAsMilliSeconds());
        }
        return connectionRetryConfig;
    }
/*
    private SerializerConfig createProtocolBufferSerializer() {
        return new SerializerConfig().setImplementation(new HzProtobufferSerializer()).setTypeClass(GeneratedMessageV3.class);
    }
*/
    private void getHealthCheckListener(LifecycleEvent event) {
        if (hazelcastStatusProperties.containsErrorStatus(event)) {
            HzHealthChecker.isAlive = false;
            log.error("HAZELCAST CONNECTION REFUSED >> {}", event.getState());
        } else {
            HzHealthChecker.isAlive = true;
            log.info("HAZELCAST CONNECTION SUCCESSFUL >> {}", event.getState());
        }
    }
}
