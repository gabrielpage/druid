package io.druid.indexing.kafka.gamesparks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.druid.data.input.MapBasedInputRow;

public class GSInputRow extends MapBasedInputRow {
	
	long timestamp;

	List<String> gameDimensions = new ArrayList<>();
	List<String> platformDimensions;
	
	Set<String> gameMetrics = new HashSet<>();
	Set<String> metrics = new HashSet<>();
	
	Map<String, Object> gameData = new HashMap<>();
	Map<String, Object> platformData;
	
	public GSInputRow(long timestamp, List<String> dimensions, Map<String, Object> event, Set<String> metrics) {
		super(timestamp, dimensions, event);
		this.metrics = metrics;
	}

	public GSInputRow(long timestamp) {
		super(timestamp, new ArrayList<String>(), new HashMap<String, Object>());
		platformData = super.getEvent();
		platformDimensions = super.getDimensions();
		this.timestamp = timestamp;
		
	}
	
	void addGameDimension(String dimension, Object value){
		gameDimensions.add(dimension);
		gameData.put(dimension, value);
	}
	
	void addPlatformDimension(String dimension, Object value){
		platformDimensions.add(dimension);
		platformData.put(dimension, value);
	}
	
	void addGameMetric(String dimension, Object value){
		gameMetrics.add(dimension);
		gameData.put(dimension, value);
	}
	
	void addPlatformMetric(String dimension, Object value){
		metrics.add(dimension);
		platformData.put(dimension, value);
	}
	
	void addSharedMetric(String dimension, Object value){
		addGameMetric(dimension, value);
		addPlatformMetric(dimension, value);
	}
	
	void addSharedDimension(String dimension, Object value){
		addGameDimension(dimension, value);
		addPlatformDimension(dimension, value);
	}
	
	MapBasedInputRow getGameInputRow(){
		return new GSInputRow(timestamp, gameDimensions, gameData, gameMetrics);
	}
	
	public Set<String> getMetrics() {
		return metrics;
	}
	

}
