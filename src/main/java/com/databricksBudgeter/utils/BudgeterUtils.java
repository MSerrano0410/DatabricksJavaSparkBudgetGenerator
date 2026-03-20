package main.java.com.databricksBudgeter.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Generic static utility functions for BudgetBricks App.
 */
public class BudgeterUtils {
	/**
	 * Converts args array to String array, instead of casting String before all args[] elements
	 * @param args - Object[] to convert.
	 * @return String[] from Object[]
	 */
	public static String[] toStringArray(Object[] args) {
		String[] argsStr = new String[args.length];
		for (int n = 0; n < args.length; n++) {
			argsStr[n] = args[n].toString();
		}
		return argsStr;
	}
	
	/**
	 * Checks if column name exists in a given Dataset
	 * @param df - Dataset to check
	 * @param columnName - column name to check
	 * @return true if column name exists in Dataset, false otherwise
	 */
	public static boolean columnExists(Dataset<Row> df, String columnName) {
		StructType schema = df.schema();
		List<String> columnNames = Arrays.stream(schema.fields())
				.map(StructField::name)
				.collect(Collectors.toList());
		return columnNames.contains(columnName);		
	}
	
	/**
	 * Creates string with a specified length of only space characters
	 * @param length
	 * @return new string with "length" number of space characters.
	 */
	public static String createSpaceString(Integer length) {
		String spaceStr = "";
		for (int i = 0; i < length; i++) {
			spaceStr += " ";
		}
		return spaceStr;
	}
}
