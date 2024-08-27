package io.github.oasisframework.hazelcast.five.helper.manager;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
		IMap<K,V> map = hazelcastInstance.getMap(mapName);
		return map.get(key);
	}

	@Override
	public <K, V> void addValueToMap(String mapName, K key, V value) {
		hazelcastInstance.getMap(mapName).set(key, value);
	}

	public boolean contains(String mapName, String key){
		try{
			IMap<?,?> map = hazelcastInstance.getMap(mapName);
			return map.containsKey(key);
		}catch (RuntimeException ex){
			return false;
		}
	}
}
