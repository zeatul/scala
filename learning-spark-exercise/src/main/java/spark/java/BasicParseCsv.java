package spark.java;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.StringReader;
import java.io.StringWriter;

public class BasicParseCsv {
	
	public static class ParseLine implements Function<String,String[]>{

		@Override
		public String[] call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			return reader.readNext();
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

		JavaSparkContext sc = new JavaSparkContext(master, "basicloadjson",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));
		
		JavaRDD<String> csvFile1 =sc.textFile(fileName);
		JavaRDD<String[]> csvData = csvFile1.map(new ParseLine());
		
		 
	}

}
