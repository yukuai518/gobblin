package gobblin.compaction.listeners;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;

import gobblin.annotation.Alias;
import gobblin.configuration.State;
import gobblin.compaction.dataset.Dataset;


public class SimpleCompactorCompletionListener implements CompactorCompletionListener {
  private static final Logger logger = LoggerFactory.getLogger (SimpleCompactorCompletionListener.class);

  private SimpleCompactorCompletionListener (State state) {
  }

  public void onCompactionCompletion (DateTime initializeTime, Set<Dataset> datasets) {
    logger.info(String.format("Compaction (started on : %s) is finished", initializeTime));
  }

  @Alias("SimpleCompactorCompletionHook")
  public static class Factory implements CompactorCompletionListenerFactory {
    @Override public CompactorCompletionListener createCompactorCompactionListener (State state) {
      return new SimpleCompactorCompletionListener (state);
    }
  }
}
