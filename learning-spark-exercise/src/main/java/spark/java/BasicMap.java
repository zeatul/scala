package spark.java;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Example 3-28. Java squaring the values in an RDD
 * 
 * @author pzhang1
 *
 */
public class BasicMap {
	public static void main(String[] args) throws Exception {
		String master;
		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local";
		}

		JavaSparkContext sc = new JavaSparkContext(master, "BasicMap",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			public Integer call(Integer x) {
				return x * x;
			}
		});
	}
}
