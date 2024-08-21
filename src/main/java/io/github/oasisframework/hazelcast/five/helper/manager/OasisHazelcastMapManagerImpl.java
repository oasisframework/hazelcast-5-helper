package io.github.oasisframework.hazelcast.five.helper.manager;

import com.hazelcast.core.HazelcastInstance;
import io.github.oasisframework.hazelcast.common.manager.OasisHazelcastMapManager;
import org.springframework.stereotype.Service;

@Service
public class OasisHazelcastMapManagerImpl implements OasisHazelcastMapManager {
	private final HazelcastInstance hazelcastInstance;

	public OasisHazelcastMapManagerImpl(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	public <K, V> V getValueFromMap(String mapName, K key) {
		return null;
	}

	@Override
	public <K, V> void addValueToMap(String mapName, K key, V value) {

	}
}
