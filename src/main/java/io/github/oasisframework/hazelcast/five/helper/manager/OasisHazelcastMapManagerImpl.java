package io.github.oasisframework.hazelcast.five.helper.manager;

import com.hazelcast.core.HazelcastInstance;
import io.github.oasisframework.hazelcast.common.manager.OasisHazelcastMapManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OasisHazelcastMapManagerImpl implements OasisHazelcastMapManager {
	private final HazelcastInstance hazelcastInstance;

	public OasisHazelcastMapManagerImpl(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	public <K, V> V getValueFromMap(String mapName, K key) {

		return (V) hazelcastInstance.getMap(mapName).get(key);

	}

	@Override
	public <K, V> void addValueToMap(String mapName, K key, V value) {
		hazelcastInstance.getMap(mapName).set(key, value);
	}
	public boolean contains(String mapName, String key){
		try{
			return hazelcastInstance.getMap(mapName).containsKey(key);
		}catch (RuntimeException ex){
			log.error("Exception occurred contains(mapName, key). Map: {}, Key: {}", mapName, key);
			return false;
		}
	}
}
