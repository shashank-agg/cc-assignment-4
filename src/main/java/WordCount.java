import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 */
public class WordCount {

	public static void main(String[] args) throws Exception {

		final String inputPath;
		final String outputPath;

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		inputPath = params.get("input");
		outputPath = params.get("output");
		if(inputPath == null || outputPath == null) {
			System.err.println("Specify 'input' and 'output'");
			return;
		}
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> text;
		text = env.readTextFile(inputPath);

		DataSet<Tuple2<String, Integer>> counts =
			text.flatMap(new Tokenizer())
			.groupBy(0)
			.sum(1);

		// emit result
		counts.writeAsCsv(outputPath, WriteMode.OVERWRITE);

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0 && isAlphabetic(token)) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	private static boolean isAlphabetic(String str) {
        return str.matches("[a-zA-Z]+");
    }

}