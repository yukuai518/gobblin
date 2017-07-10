package gobblin.compaction.mapreduce;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.suite.CompactionSuite;
import gobblin.compaction.suite.CompactionSuiteUtils;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.dataset.Dataset;
import gobblin.metrics.event.EventSubmitter;
import gobblin.runtime.TaskContext;
import gobblin.runtime.mapreduce.MRTask;

import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapreduce.Job;

/**
 * Customized task of type {@link MRTask}, which runs MR job to compact dataset.
 * The job creation is delegated to {@link CompactionSuite#createJob(Dataset)}
 * After job is created, {@link MRCompactionTask#run()} is invoked and after compaction is finished.
 * a callback {@link CompactionSuite#getCompactionCompleteActions()} will be invoked
 */
@Slf4j
public class MRCompactionTask extends MRTask {
  protected final CompactionSuite suite;
  protected final Dataset dataset;
  protected final EventSubmitter eventSubmitter;
  /**
   * Constructor
   */
  public MRCompactionTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    this.suite = CompactionSuiteUtils.getCompactionSuiteFactory (taskContext.getTaskState()).
            createSuite(taskContext.getTaskState());
    this.dataset = this.suite.load(taskContext.getTaskState());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.MRCompactionTask")
        .addMetadata(additionalEventMetadata()).build();
  }

  /**
   * Below three steps are performed for a compaction task:
   * Do verifications before a map-reduce job is launched.
   * Start a map-reduce job and wait until it is finished
   * Do post-actions after map-reduce job is finished
   */
  @Override
  public void run() {
    List<CompactionVerifier> verifiers = this.suite.getMapReduceVerifiers();
    for (CompactionVerifier verifier : verifiers) {
      if (!verifier.verify(dataset)) {
        log.error("Verification {} for {} is not passed.", verifier.getName(), dataset.datasetURN());
        this.onMRTaskComplete (false, new IOException("Compaction verification for MR is failed"));
        return;
      }
    }

    super.run();
  }

  public void onMRTaskComplete (boolean isSuccess, Throwable throwable) {
    if (isSuccess) {
      try {
        List<CompactionCompleteAction> actions = this.suite.getCompactionCompleteActions();
        for (CompactionCompleteAction action: actions) {
          action.addEventSubmitter(eventSubmitter);
          action.onCompactionJobComplete(dataset);
        }
        submitEvent(CompactionSlaEventHelper.COMPACTION_COMPLETED_EVENT_NAME);
        super.onMRTaskComplete(true, null);
      } catch (IOException e) {
        submitEvent(CompactionSlaEventHelper.COMPACTION_FAILED_EVENT_NAME);
        super.onMRTaskComplete(false, e);
      }
    } else {
      submitEvent(CompactionSlaEventHelper.COMPACTION_FAILED_EVENT_NAME);
      super.onMRTaskComplete(false, throwable);
    }
  }

  private void submitEvent(String eventName) {
    Map<String, String> eventMetadataMap = new HashMap<>();
    eventMetadataMap.put("datasetUrn", this.dataset.datasetURN());
    this.eventSubmitter.submit(eventName, eventMetadataMap);
  }

  /**
   * Create a map-reduce job
   * The real job configuration is delegated to {@link CompactionSuite#createJob(Dataset)}
   *
   * @return a map-reduce job
   */
  protected Job createJob() throws IOException {
    return this.suite.createJob(dataset);
  }
}
