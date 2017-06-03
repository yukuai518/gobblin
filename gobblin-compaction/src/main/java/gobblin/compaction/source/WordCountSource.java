package gobblin.compaction.source;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.mapreduce.MRCompactionTaskFactory;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.WordCountTaskFactory;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.runtime.JobState;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.FileBasedSource;
import gobblin.source.extractor.hadoop.HadoopFsHelper;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;


@Slf4j
public class WordCountSource extends FileBasedSource<String, String> {
  public static final String WORDCOUNT_JAR_SUBDIR = "_wordcount";
  public static final String INPUT_DIRECTORIES_KEY = "input.directories";
  public static final String OUTPUT_LOCATION = "output.location";
  public static final String WORDCOUNT_TMP_DEST_DIR = "/tmp/ketl_dev/wordcount";
  public static final String DEFAULT_WORDCOUNT_TMP_DEST_DIR = "/tmp/ketl_dev/wordcount_default";
  private Path tmpJobDir;
  private FileSystem fs;
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);
    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    try  {
      this.fs = getSourceFileSystem(state);
      initJobDir(state);
      copyJarDependencies (state);
    }catch (IOException e) {
      log.error("Error occurred when initializing job directory");
    }
    log.info("Getting work units");
    List<WorkUnitState> previousWorkunits = Lists.newArrayList(state.getPreviousWorkUnitStates());
    Set<String> prevFsSnapshot = Sets.newHashSet();

    // Get list of files seen in the previous run
    if (!previousWorkunits.isEmpty()) {
      if (previousWorkunits.get(0).getWorkunit().contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
        prevFsSnapshot =
            previousWorkunits.get(0).getWorkunit().getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
      } else if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED,
          ConfigurationKeys.DEFAULT_SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED)) {
        // If a previous job exists, there should be a snapshot property.  If not, we need to fail so that we
        // don't accidentally read files that have already been processed.
        throw new RuntimeException(String.format("No '%s' found on state of prior job",
            ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT));
      }
    }

    List<WorkUnit> workUnits = Lists.newArrayList();
    List<WorkUnit> previousWorkUnitsForRetry = this.getPreviousWorkUnitsForRetry(state);
    log.info("Total number of work units from the previous failed runs: " + previousWorkUnitsForRetry.size());
    for (WorkUnit previousWorkUnitForRetry : previousWorkUnitsForRetry) {
      prevFsSnapshot.addAll(previousWorkUnitForRetry.getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
      workUnits.add(previousWorkUnitForRetry);
    }

    // Get list of files that need to be pulled
    List<String> currentFsSnapshot = this.getcurrentFsSnapshot(state);
    HashSet<String> filesWithTimeToPull = new HashSet<>(currentFsSnapshot);
    filesWithTimeToPull.removeAll(prevFsSnapshot);
    log.info ("##### Excluding " + prevFsSnapshot);
    List<String> filesToPull = new ArrayList<>();
    Iterator<String> it = filesWithTimeToPull.iterator();
    while (it.hasNext()) {
      String filesWithoutTimeToPull[] = it.next().split(this.splitPattern);
      filesToPull.add(filesWithoutTimeToPull[0]);
    }

    if (!filesToPull.isEmpty()) {

      int numPartitions = state.contains(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS)
          && state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) <= filesToPull.size()
          ? state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) : filesToPull.size();
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("The number of partitions should be positive");
      }

      int filesPerPartition = filesToPull.size() % numPartitions == 0 ? filesToPull.size() / numPartitions
          : filesToPull.size() / numPartitions + 1;

      // Distribute the files across the workunits
      for (int fileOffset = 0; fileOffset < filesToPull.size(); fileOffset += filesPerPartition) {
        SourceState partitionState = new SourceState();
        partitionState.addAll(state);

        // Eventually these setters should be integrated with framework support for generalized watermark handling
        partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT,
            StringUtils.join(currentFsSnapshot, ","));

        List<String> partitionFilesToPull = filesToPull.subList(fileOffset,
            fileOffset + filesPerPartition > filesToPull.size() ? filesToPull.size() : fileOffset + filesPerPartition);
        partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
            StringUtils.join(partitionFilesToPull, ","));

        // Add files to workunit

        workUnits.add(createWorkUnit(partitionState));
      }

      log.info("Total number of work units for the current run: " + (workUnits.size() - previousWorkUnitsForRetry.size()));
    }

    return workUnits;
  }

  private WorkUnit createWorkUnit (SourceState state) {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, WordCountTaskFactory.class);
    workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
    workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT, state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT));
    return workUnit;
  }

  public void initFileSystemHelper (State state) throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown (SourceState state) {
    try {
      boolean f = fs.delete(this.tmpJobDir, true);
      log.info("Job dir is removed from {} with status {}", this.tmpJobDir, f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = Lists.newArrayList();
    String path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);

    try {
      log.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
    } catch (FileBasedHelperException e) {
      log.error("Not able to run ls command due to " + e.getMessage() + " will not pull any files", e);
    }
    return results;
  }

  protected FileSystem getSourceFileSystem(State state)
      throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  /**
   * Copy dependent jars to a temporary job directory on HDFS
   */
  private void copyJarDependencies (State state) throws IOException {
    if (this.tmpJobDir == null) {
      throw new RuntimeException("Job directory is not created");
    }

    if (!state.contains(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      return;
    }

    // create sub-dir to save jar files
    LocalFileSystem lfs = FileSystem.getLocal(HadoopUtils.getConfFromState(state));
    Path tmpJarFileDir = new Path(this.tmpJobDir, WORDCOUNT_JAR_SUBDIR);
    this.fs.mkdirs(tmpJarFileDir);
    state.setProp (MRCompactor.COMPACTION_JARS, tmpJarFileDir.toString());

    // copy jar files to hdfs
    for (String jarFile : state.getPropAsList(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      for (FileStatus status : lfs.globStatus(new Path(jarFile))) {
        Path tmpJarFile = new Path(this.fs.makeQualified(tmpJarFileDir), status.getPath().getName());
        this.fs.copyFromLocalFile(status.getPath(), tmpJarFile);
        log.info(String.format("%s will be added to classpath", tmpJarFile));
      }
    }
  }

  private void initJobDir (SourceState state) throws IOException {
    String tmpBase = state.getProp(WORDCOUNT_TMP_DEST_DIR, DEFAULT_WORDCOUNT_TMP_DEST_DIR);
    String jobId;

    if (state instanceof JobState) {
      jobId = ((JobState) state).getJobId();
    } else {
      jobId = UUID.randomUUID().toString();
    }

    this.tmpJobDir = new Path(tmpBase, jobId);
    this.fs.mkdirs(this.tmpJobDir);
    state.setProp (MRCompactor.COMPACTION_JOB_DIR, tmpJobDir.toString());
    log.info ("Job dir is created under {}", this.tmpJobDir);
  }

}