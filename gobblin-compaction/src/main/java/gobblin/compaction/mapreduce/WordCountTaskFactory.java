package gobblin.compaction.mapreduce;

import gobblin.publisher.DataPublisher;
import gobblin.publisher.NoopPublisher;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskContext;
import gobblin.runtime.task.TaskFactory;
import gobblin.runtime.task.TaskIFace;


public class WordCountTaskFactory implements TaskFactory {

  public TaskIFace createTask(TaskContext taskContext) {
    return new WordCountTask (taskContext);
  }

  @Override
  public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
    return new NoopPublisher(datasetState);
  }
}
