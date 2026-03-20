package main.java.com.databricksBudgeter;

import org.apache.spark.sql.SparkSession;

public class BudgeterContext {
	private SparkSession spark;
	
	private String appName;
	
	public BudgeterContext(String[] args) {
		try {
			this.init(args);
		} catch (Exception e) {
			System.out.println("Could not initialize BudgeterContext due to: " + e);
		}
	}
	
	private void init(String[] args) throws Exception {
		//initialize variables for budgeter (including BudgeterJob class)
	}
	
	/**
	 * Creates Spark session or uses existing session if available
	 * @return true if Spark session created or used (if existing), false if error occurred.
	 */
	public boolean createSparkSession() {
		try {
			this.spark = SparkSession.active();
			System.out.println("Using existing Spark session");
		} catch (Exception e) {
			try {
				System.out.println("Creating new Spark session.");
				this.spark = SparkSession.builder().appName(this.appName).getOrCreate();
				
				this.spark.conf().set("spark.sql.sources.partitionOverwriteMode", "static");
				this.spark.conf().set("spark.sql.adaptive.enable", false);
				this.spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", true);
				this.spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1);
			} catch (Exception sparkException) {
				return false;
			}
		}
		
		System.out.println("Spark session created!");
		return true;
	}
}
