package io.github.oasisframework.hazelcast.five.helper.connection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import io.github.oasisframework.hazelcast.common.properties.HazelcastConnectionAbstractProperties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConnection {

    public static final String HAZELCAST_INSTANCE = "hazelcastInstance";
    private static final String MONITORING_LEVEL_KEY = "hazelcast.health.monitoring.level";
    private static final String MONITORING_LEVEL_VALUE = "NOISY";
    private static final String INVOCATION_TIMEOUT_KEY = "hazelcast.client.invocation.timeout.seconds";
    private static final String INVOCATION_TIMEOUT_VALUE = "1";
    private static final int ZERO_TIMEOUT_VALUE = 0;

    private final HazelcastConnectionAbstractProperties hazelcastConnectionProperties;
    private final SerializerConfig customConfig;

    public HazelcastConnection(HazelcastConnectionAbstractProperties hazelcastConnectionProperties, SerializerConfig customConfig) {
        this.hazelcastConnectionProperties = hazelcastConnectionProperties;
        this.customConfig = customConfig;
    }

    @Bean(HAZELCAST_INSTANCE)
    public HazelcastInstance hazelCastClient() {
		return HazelcastClient.newHazelcastClient(createClientConfig());
    }

    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();

        if (StringUtils.isBlank(hazelcastConnectionProperties.getConnectionName()) || StringUtils.isBlank(
                hazelcastConnectionProperties.getAddress())) {
            return config;
        }

        config.setNetworkConfig(createClientNetworkConfig());
        config.setConnectionStrategyConfig(createClientConnectionStrategyConfig(config));
        config.getSerializationConfig().addSerializerConfig(customConfig);

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
            connectionRetryConfig.setClusterConnectTimeoutMillis(ConnectionRetryConfig.DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS);
        } else {
            connectionRetryConfig.setClusterConnectTimeoutMillis(hazelcastConnectionProperties.getConnectTimeoutAsMilliSeconds());
        }
        return connectionRetryConfig;
    }

}
