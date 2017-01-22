package io.druid.indexing.kafka.gamesparks;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.metamx.common.Granularity;
import com.metamx.common.logger.Logger;

import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.KafkaIOConfig;
import io.druid.indexing.kafka.KafkaTuningConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.appenderator.UsedSegmentChecker;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class GSSharedAppenderatorDriver extends FiniteAppenderatorDriver {

	private static final Logger log = new Logger(GSSharedAppenderatorDriver.class);
	
	int itemsAdded = 0;
	
	ConcurrentHashMap<String, FiniteAppenderatorDriver> mappedDrivers = new ConcurrentHashMap<>();
	FiniteAppenderatorDriver platformDriver;

	Appenderator appenderator;
	SegmentAllocator segmentAllocator;
	SegmentHandoffNotifierFactory handoffNotifierFactory;
	UsedSegmentChecker usedSegmentChecker;
	ObjectMapper objectMapper;
	int maxRowsPerSegment;
	long handoffConditionTimeout;
	DataSchema dataSchema_0;

	private final KafkaTuningConfig tuningConfig;
	private final KafkaIOConfig ioConfig;
	FireDepartmentMetrics metrics;
	TaskToolbox toolbox;

	public GSSharedAppenderatorDriver(Appenderator appenderator, SegmentAllocator segmentAllocator,
			SegmentHandoffNotifierFactory handoffNotifierFactory, UsedSegmentChecker usedSegmentChecker,
			ObjectMapper objectMapper, int maxRowsPerSegment, long handoffConditionTimeout, DataSchema dataSchema,
			KafkaTuningConfig tuningConfig, KafkaIOConfig ioConfig, FireDepartmentMetrics metrics,
			TaskToolbox toolbox) {
		
		super(appenderator, 
				segmentAllocator, 
				handoffNotifierFactory, 
				usedSegmentChecker, 
				objectMapper,
				maxRowsPerSegment, 
				handoffConditionTimeout				
				);
		
		this.appenderator = appenderator;
		this.segmentAllocator = segmentAllocator;
		this.handoffNotifierFactory = handoffNotifierFactory;
		this.usedSegmentChecker = usedSegmentChecker;
		this.objectMapper = objectMapper;
		this.maxRowsPerSegment = maxRowsPerSegment;
		this.handoffConditionTimeout = handoffConditionTimeout;
		this.dataSchema_0 = dataSchema;
		this.tuningConfig = tuningConfig;
		this.ioConfig = ioConfig;
		this.metrics = metrics;
		this.toolbox = toolbox;
		
		platformDriver = createDriver(null);
	}

	@Override
	public SegmentIdentifier add(InputRow row, String sequenceName, Supplier<Committer> committerSupplier)
			throws IOException {
			SegmentIdentifier ret = platformDriver.add(row, sequenceName, committerSupplier);
			if (row.getDimension("apiKey") != null && row.getDimension("apiKey").size() == 1) {
				String gameId = row.getDimension("apiKey").get(0);
				row = ((GSInputRow)row).getGameInputRow();
				row.getDimensions().remove("apiKey");
				if (!mappedDrivers.containsKey(gameId)) {
					
					FiniteAppenderatorDriver gameFiniteAppenderatorDriver = createDriver(gameId);
					if(mappedDrivers.putIfAbsent(gameId, gameFiniteAppenderatorDriver) == null){
						gameFiniteAppenderatorDriver.startJob();
					}
				}
				return mappedDrivers.get(gameId).add(row, sequenceName+ "-" + gameId, committerSupplier);
			}
			return ret;
	}

	private FiniteAppenderatorDriver createDriver(String gameId) {
		GSMetricAwareDataSchema gameDataSchema = new GSMetricAwareDataSchema(dataSchema_0.getDataSource() + (gameId != null ? ("-" + gameId) : ""),
				dataSchema_0.getParserMap(), dataSchema_0.getAggregators(), dataSchema_0.getGranularitySpec(),
				objectMapper);
		
		Appenderator gameAppenderator = newAppenderator(gameDataSchema, gameId);
		gameAppenderator.startJob();
		FiniteAppenderatorDriver gameFiniteAppenderatorDriver = newDriver(gameAppenderator, toolbox, gameDataSchema);
		return gameFiniteAppenderatorDriver;
	}

	private Appenderator newAppenderator(DataSchema gameDataSchema, String gameId) {
		final int maxRowsInMemoryPerPartition = (tuningConfig.getMaxRowsInMemory()
				/ ioConfig.getStartPartitions().getPartitionOffsetMap().size());
		
		return Appenderators.createRealtime(gameDataSchema,
				tuningConfig.withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist-" + gameId))
						.withMaxRowsInMemory(maxRowsInMemoryPerPartition),
				metrics, toolbox.getSegmentPusher(), toolbox.getObjectMapper(), toolbox.getIndexIO(),
				tuningConfig.getBuildV9Directly() ? toolbox.getIndexMergerV9() : toolbox.getIndexMerger(),
				toolbox.getQueryRunnerFactoryConglomerate(), toolbox.getSegmentAnnouncer(), toolbox.getEmitter(),
				toolbox.getQueryExecutorService(), toolbox.getCache(), toolbox.getCacheConfig());
	}

	private FiniteAppenderatorDriver newDriver(final Appenderator appenderator, final TaskToolbox toolbox,
			GSMetricAwareDataSchema gameDataSchema) {
		return new GSMetricFiniteAppenderatorDriver(appenderator,
				new GameActionBasedSegmentAllocator(toolbox.getTaskActionClient(), gameDataSchema),
				toolbox.getSegmentHandoffNotifierFactory(),
				new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()), toolbox.getObjectMapper(),
				tuningConfig.getMaxRowsPerSegment(), tuningConfig.getHandoffConditionTimeout(), gameDataSchema);
			
	}

	@Override
	public Object startJob() {
		return platformDriver.startJob();
	}

	@Override
	public void clear() throws InterruptedException {
		platformDriver.clear();
		for(FiniteAppenderatorDriver driver : mappedDrivers.values()){
			log.info("clear 2");
			driver.clear();
		}
	}

	@Override
	public Object persist(Committer committer) throws InterruptedException {
		
		for(FiniteAppenderatorDriver driver : mappedDrivers.values()){
			driver.persist(committer);
		}
		return platformDriver.persist(committer);
	}
	
	@Override
	public SegmentsAndMetadata finish(final TransactionalSegmentPublisher publisher, final Committer committer)
			throws InterruptedException {

		ExecutorService es = Executors.newFixedThreadPool(mappedDrivers.values().size());

		for (final String dataSource : mappedDrivers.keySet()) {
			final FiniteAppenderatorDriver driver = mappedDrivers.get(dataSource);
			es.submit(new Runnable() {

				@Override
				public void run() {
					try {
						SegmentsAndMetadata innerRet = driver.finish(publisher, committer);
						driver.close();
						log.info("inner finish [%s] \n [%s]", dataSource, innerRet.getCommitMetadata());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});

		}
		SegmentsAndMetadata ret = platformDriver.finish(publisher, committer);
		es.shutdown();
		es.awaitTermination(60, TimeUnit.MINUTES);

		return ret;

	}

	@Override
	public void close() {
		log.info("close 1");
		platformDriver.close();
		for(FiniteAppenderatorDriver driver : mappedDrivers.values()){
			log.info("close 2");
			driver.close();
		}
		log.info("close 3");
		
	}
	
	
	
}




