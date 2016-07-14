package spark.java;

import java.io.StringReader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class BasicLoadWholeCsv {

	public static class ParseLine implements
			FlatMapFunction<Tuple2<String, String>, String[]> {

		@Override
		public Iterable<String[]> call(Tuple2<String, String> file)
				throws Exception {
			StringReader stringReader = new StringReader(file._2());
			CSVReader reader = new CSVReader(stringReader);
			return reader.readAll();
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			throw new Exception(
					"Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
		}
		String master = args[0];
		String fileName = args[1];
		String outfile = args[2];
		final String key = args[3];

		JavaSparkContext sc = new JavaSparkContext(master, "basicLoadWholeCsv",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));

		JavaPairRDD<String, String> csvData = sc.wholeTextFiles(fileName);
		JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());
		JavaRDD<String[]> result = keyedRDD
				.filter(new Function<String[], Boolean>() {

					@Override
					public Boolean call(String[] input) throws Exception {
						return input[0].equals(key);
					}
				});
		
		 result.saveAsTextFile(outfile);
	}

}
