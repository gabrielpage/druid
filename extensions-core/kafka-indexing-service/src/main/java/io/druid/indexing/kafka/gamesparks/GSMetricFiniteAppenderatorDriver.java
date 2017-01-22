package io.druid.indexing.kafka.gamesparks;

import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;

import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.UsedSegmentChecker;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;

public class GSMetricFiniteAppenderatorDriver extends FiniteAppenderatorDriver {

	GSMetricAwareDataSchema dataSchema;
	
	private static final Logger log = new Logger(GSMetricFiniteAppenderatorDriver.class);
	
	public GSMetricFiniteAppenderatorDriver(Appenderator appenderator, SegmentAllocator segmentAllocator,
			SegmentHandoffNotifierFactory handoffNotifierFactory, UsedSegmentChecker usedSegmentChecker,
			ObjectMapper objectMapper, int maxRowsPerSegment, long handoffConditionTimeout, GSMetricAwareDataSchema dataSchema) {
		super(appenderator, segmentAllocator, handoffNotifierFactory, usedSegmentChecker, objectMapper, maxRowsPerSegment,
				handoffConditionTimeout);
		this.dataSchema = dataSchema;
	}
	
	@Override
	public SegmentIdentifier add(InputRow row, String sequenceName, Supplier<Committer> committerSupplier)
			throws IOException {
		if(row instanceof GSInputRow){
			Set<String> metrics = ((GSInputRow)row).getMetrics(); 
			for(String metric : metrics){
				if(!dataSchema.getAggregatorMap().containsKey(metric)){
					Object value = row.getDimension(metric).get(0);
					if(value instanceof String){
						log.info("ADDING STRING METRIC " + metric + " TO " + dataSchema.getDataSource());
						dataSchema.getAggregatorMap().put(metric, new HyperUniquesAggregatorFactory(metric, metric));
					} else if(value instanceof Number){
						log.info("ADDING NUMBER METRIC " + metric + " TO " + dataSchema.getDataSource());
						dataSchema.getAggregatorMap().put(metric, new LongSumAggregatorFactory(metric, metric));
						dataSchema.getAggregatorMap().put(metric, new LongMaxAggregatorFactory(metric+"_MAX", metric));
						dataSchema.getAggregatorMap().put(metric, new LongMinAggregatorFactory(metric+"_MIN", metric));
					} else {
						log.info("VALUE IS " + value.getClass().getName());
					}
				}
			}
		} else {
			log.info("ROW IS NOT GSInputRow " + row.getClass().getName());
		}
		return super.add(row, sequenceName, committerSupplier);
	}

}
