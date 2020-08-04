import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class DocFileInputFormat extends CombineFileInputFormat<Text, Text> {
  public DocFileInputFormat(){
    super();
    setMaxSplitSize(SplitSize.getInstance().getSplitSize());
  }

  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<Text, Text>((CombineFileSplit)inputSplit, context, DocRecordReader.class);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file){
    return false; // not split file into half
  }
}
