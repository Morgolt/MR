import com.epam.training.bigdata.UserAgentMapper;
import com.epam.training.bigdata.UserAgentReducer;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class UserAgentMRTests {

  private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
  private ReduceDriver<Text, LongWritable, Text, NullWritable> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, NullWritable> mrDriver;

  @Before
  public void setUp() {
    UserAgentMapper mapper = new UserAgentMapper();
    UserAgentReducer reducer = new UserAgentReducer();
    mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
    reduceDriver = new ReduceDriver<Text, LongWritable, Text, NullWritable>(reducer);
    mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    Text val1 = new Text(
        "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1"
            + "\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; "
            + "+http://yandex.com/bots)\"");
    mapDriver.withInput(new LongWritable(), val1);
    mapDriver.withOutput(new Text("ip1"), new LongWritable(40028L));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException {
    Text ip1 = new Text("ip1");
    LongWritable bytes1 = new LongWritable(10L);
    LongWritable bytes2 = new LongWritable(20L);
    ArrayList<LongWritable> bytes = new ArrayList<LongWritable>();
    bytes.add(bytes1);
    bytes.add(bytes2);
    reduceDriver.withInput(ip1, bytes);
    reduceDriver.withOutput(new Text("ip1,15.0,30"), NullWritable.get());
    reduceDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    Text in1 = new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"");
    Text in2 = new Text("ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 50 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"");
    Text in3 = new Text("ip2 - - [24/Apr/2011:04:14:36 -0400] \"GET /~strabal/grease/photo9/927-5.jpg HTTP/1.1\" 200 42011 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"");
    Text out1 = new Text("ip2,42011.0,42011");
    Text out2 = new Text("ip1,45.0,90");
    mrDriver.withInput(new LongWritable(), in1);
    mrDriver.withInput(new LongWritable(), in2);
    mrDriver.withInput(new LongWritable(), in3);
    mrDriver.withOutput(out1, NullWritable.get());
    mrDriver.withOutput(out2, NullWritable.get());
    mrDriver.runTest();
  }

  @Test
  public void testCounters() {

  }

}
