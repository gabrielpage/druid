package io.druid.indexing.kafka.gamesparks;

import java.io.IOException;
import java.lang.reflect.Method;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.common.Granularity;

import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.gamesparks.GameActionBasedSegmentAllocator.GameSegmentAllocateAction;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

class  GameActionBasedSegmentAllocator implements SegmentAllocator
{
  private final TaskActionClient taskActionClient;
  private final DataSchema dataSchema_1;

  public GameActionBasedSegmentAllocator(
      TaskActionClient taskActionClient,
      DataSchema dataSchema
  )
  {
    this.taskActionClient = taskActionClient;
    this.dataSchema_1 = dataSchema;
  }

  @Override
  public SegmentIdentifier allocate(
      final DateTime timestamp,
      final String sequenceName,
      final String previousSegmentId
  ) throws IOException
  {
    return taskActionClient.submit(
        new GameSegmentAllocateAction(
        		dataSchema_1.getDataSource(),
            timestamp,
            dataSchema_1.getGranularitySpec().getQueryGranularity(),
            dataSchema_1.getGranularitySpec().getSegmentGranularity(),
            sequenceName,
            previousSegmentId
        )
    );
  }
  
  @JsonTypeName("segmentAllocate")
  class GameSegmentAllocateAction extends SegmentAllocateAction {

	  	Task lastTask;
	  	Task enhancedTask;
	  	
	  	MethodInterceptor mi;
	  	
	  	public String getType(){
	  		return "segmentAllocate";
	  	}
	  	
	  	@Override
	  	public String getDataSource() {
	  		return dataSchema_1.getDataSource();
	  	}
	  
		public GameSegmentAllocateAction(String dataSource, DateTime timestamp, QueryGranularity queryGranularity,
				Granularity preferredSegmentGranularity, String sequenceName, String previousSegmentId) {
			super(dataSource, timestamp, queryGranularity, preferredSegmentGranularity, sequenceName, previousSegmentId);
		}
		
		@Override
		public SegmentIdentifier perform(Task task, TaskActionToolbox toolbox) throws IOException {
			if(lastTask != task){
				final Task finalTask = task;
				enhancedTask = (Task) Enhancer.create(Task.class, new MethodInterceptor() {
					@Override
					public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
							throws Throwable {
						System.err.println(method.getName());
						if (method.getName().equals("getDataSource")) {
							return dataSchema_1.getDataSource();
						}
						return method.invoke(finalTask, args);
					}
				});
				lastTask = task;
			}
			return super.perform(enhancedTask, toolbox);
		}
		
	}
}