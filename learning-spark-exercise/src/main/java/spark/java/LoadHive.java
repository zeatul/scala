package spark.java;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrame;

public class LoadHive {

	public static class SquareKey implements Function<Row, Integer> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Integer call(Row row) throws Exception {
			return row.getInt(0) * row.getInt(0);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			throw new Exception("Usage LoadHive sparkMaster tbl");
		}
		String master = args[0];
		String tbl = args[1];

		JavaSparkContext sc = new JavaSparkContext(master, "LoadHive",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));
		SQLContext sqlCtx = new SQLContext(sc);
		DataFrame rdd = sqlCtx.sql("SELECT key, value FROM src");
		JavaRDD<Integer> squaredKeys = rdd.toJavaRDD().map(new SquareKey());
		List<Integer> result = squaredKeys.collect();
		for (Integer elem : result) {
			System.out.println(elem);
		}
	}

}
