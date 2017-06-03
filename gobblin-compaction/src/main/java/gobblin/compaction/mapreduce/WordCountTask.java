package gobblin.compaction.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.source.WordCountSource;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.runtime.TaskContext;
import gobblin.runtime.TaskState;
import gobblin.runtime.mapreduce.MRTask;
import gobblin.util.HadoopUtils;


@Slf4j
public class WordCountTask extends MRTask {
  TaskContext context;
  TaskState state;
  FileSystem fs;
  Path mrOutputDir;
  Path finalOutput;
  String files;
  public WordCountTask(TaskContext taskContext) {
    super(taskContext);
    context = taskContext;
    state = context.getTaskState();
  }
  public void run() {
    super.run();
    try {
      Path target = new Path(finalOutput, new Path(files).getName());
      if (fs.exists(target)) {
        log.info ("#### target " + target + " exists");
        return;
      }
      Path source = new Path(mrOutputDir, "part-r-00000");
      log.info ("### Renaming " + source.toString() + " to " + target.toString());
      this.fs.rename(source, target);
    } catch (IOException e) {
      log.error(e.toString());
    }
  }
  protected Job createJob() throws IOException {
    TaskState state = this.context.getTaskState();
    this.fs = this.getFileSystem(state);
    this.files = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
    this.finalOutput = new Path(state.getProp(WordCountSource.OUTPUT_LOCATION));
    log.info ("##### Processing file " + files);

    Configuration conf = HadoopUtils.getConfFromState(state);

    // Turn on mapreduce output compression by default
    if (conf.get("mapreduce.output.fileoutputformat.compress") == null && conf.get("mapred.output.compress") == null) {
      conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    }

    // Disable delegation token cancellation by default
    if (conf.get("mapreduce.job.complete.cancel.delegation.tokens") == null) {
      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    }

    mrOutputDir = new Path ("/tmp/ketl_dev/wordCount/" + UUID.randomUUID());
    this.fs.delete(mrOutputDir, true);
    addJars(conf);
    Job job = Job.getInstance(conf, "WordCount_" + files);
    job.setJarByClass(WordCountTask.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(files));
    FileOutputFormat.setOutputPath(job, mrOutputDir);
    log.info (files + " wordcount completed at " + mrOutputDir.toString());
    return job;
  }

  private void addJars(Configuration conf) throws IOException {
    if (!this.context.getTaskState().contains(MRCompactor.COMPACTION_JARS)) {
      return;
    }
    log.info ("=======> Add jars: " + this.context.getTaskState().getProp(MRCompactor.COMPACTION_JARS));
    Path jarFileDir = new Path(this.context.getTaskState().getProp(MRCompactor.COMPACTION_JARS));
    for (FileStatus status : this.fs.listStatus(jarFileDir)) {
      DistributedCache.addFileToClassPath(status.getPath(), conf, this.fs);
    }
  }
  // This is taken directly from
  // https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // This is taken directly from
  // https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  private FileSystem getFileSystem(State state)
      throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    return fs;
  }

  public State getPersistentState () {
    String files = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT);
    log.info ("Files saved in this run : [" + files + "]");
    return state;
  }
}
