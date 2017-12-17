import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        int sum = 0;
        Iterator valuesIt = values.iterator();

        while(valuesIt.hasNext()){
            System.out.println(valuesIt.next().toString());
            sum = sum + (Integer) valuesIt.next();
        }

        context.write(key, new IntWritable(sum));
    }
}
