package io.github.oasisframework.hazelcast.five.helper.properties;

import com.hazelcast.core.LifecycleEvent;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Setter
@Configuration
@ConfigurationProperties(prefix = "hz-status-config")
public class HazelcastStatusProperties {
    private String errorStatus = "SHUTTING_DOWN, SHUTDOWN, MERGE_FAILED, CLIENT_DISCONNECTED";

    public List<String> getErrorStatus() {
        if (errorStatus == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(errorStatus.split(","));
    }

    public boolean containsErrorStatus(LifecycleEvent event) {
        LifecycleEvent.LifecycleState state = event.getState();
        return getErrorStatus().contains(state.name());
    }
}