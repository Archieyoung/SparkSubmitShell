package org.bgi.flexlab;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.InputStream;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkSubmitShell {
    private static final Logger logger = LoggerFactory.getLogger(SparkSubmitShell.class);

    public static void main(String[] args) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream log4jPropertiestIs = classloader.getResourceAsStream("log4j.properties");
        PropertyConfigurator.configure(log4jPropertiestIs);

        if (args.length != 2) {
            System.out.println("usage: spark-submit --master [yarn|local] [options] --class org.bgi.flexlab.SparkSubmitShell spark-submit-shell.jar <sh> <args>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("VarSimSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> jobsRDD = sc.parallelize(Arrays.asList(new Integer[]{0}), 1);
        final String shell = args[0];
        final String shell_args = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
        List<Integer> returnCodes = jobsRDD.mapPartitionsWithIndex(new SparkJob(shell, shell_args), false).collect();
        if (returnCodes.get(0) == 0) {
            logger.info("job success.");
        } else {
            logger.error("job failed with return code: {}", returnCodes.get(0));
        }

        sc.close();
    }
}
