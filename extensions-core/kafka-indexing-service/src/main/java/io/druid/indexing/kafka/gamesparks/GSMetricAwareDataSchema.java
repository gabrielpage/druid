package io.druid.indexing.kafka.gamesparks;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;

public class GSMetricAwareDataSchema extends DataSchema {
	
	private static final Logger log = new Logger(GSMetricAwareDataSchema.class);
	
	private final ObjectMapper jsonMapper;
	Map<String, AggregatorFactory> aggregatorMap = new HashMap<>();
	
	public GSMetricAwareDataSchema(String dataSource, Map<String, Object> parser, AggregatorFactory[] aggregators,
			GranularitySpec granularitySpec, ObjectMapper jsonMapper) {
		super(dataSource, parser, aggregators, granularitySpec, jsonMapper);
		
		for(int i=0 ; i<aggregators.length ; i++){
			AggregatorFactory af = aggregators[i];
			this.aggregatorMap.put(af.getName(), af);
		}
		
		this.jsonMapper = jsonMapper;
	}

	@Override
	public AggregatorFactory[] getAggregators() {
		AggregatorFactory[] ret = new AggregatorFactory[aggregatorMap.values().size()] ;
		log.info("getAggregators()=" + ret);
		return aggregatorMap.values().toArray(ret);
	}
	
	@Override
	public DataSchema withGranularitySpec(GranularitySpec granularitySpec) {
		return new GSMetricAwareDataSchema(getDataSource(), getParserMap(), getAggregators(), getGranularitySpec(), jsonMapper);
	}

	public Map<String, AggregatorFactory> getAggregatorMap() {
		return aggregatorMap;
	}
	
	

}
