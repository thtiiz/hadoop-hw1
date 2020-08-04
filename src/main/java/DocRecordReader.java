import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;


public class DocRecordReader extends RecordReader<Text, Text>{
  private long startOffset;
  private long end;
  private long pos;
  private FileSystem fs;
  private Path path;
  private Text key;
  private Text value;

  private FSDataInputStream fileIn;
  private LineReader reader;

  public DocRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException{
    this.path = split.getPath(index);
    fs = this.path.getFileSystem(context.getConfiguration());
    this.startOffset = split.getOffset(index);
    this.end = startOffset + split.getLength(index);

    fileIn = fs.open(path);
    reader = new LineReader(fileIn);
    this.pos = startOffset;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {}

  @Override
  public void close() throws IOException {}

  @Override
  public float getProgress() throws IOException{
    if (startOffset == end) {
      return 0;
    }
    return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException{
    if (key == null) {
      key = new Text(path.getName());
    }
    if (value == null){
      value = new Text();
    }

    if (pos < end) {
      pos += reader.readLine(value);
      return true; // has more
    } else {
      key = null; // end file
      value = null;
      return false;
    }

  }
}