package org.bgi.flexlab;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
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

        SparkSubmitShellOptions options = new SparkSubmitShellOptions();
        options.parse(args);
        String[] positionalArgs = options.getPositionalArgs();
        if (positionalArgs.length < 1) {
            logger.error("must supply at least on positional argument as job shell script file.");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("VarSimSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> jobsRDD = sc.parallelize(Arrays.asList(new Integer[]{0}), 1);
        String shell = positionalArgs[0];
        File shellFile = new File(shell);
        shell = shellFile.getAbsolutePath();
        String shell_args = "";
        if (positionalArgs.length > 1) {
            shell_args = String.join(" ", Arrays.copyOfRange(positionalArgs, 1, args.length));
        }
        List<Integer> returnCodes = jobsRDD.mapPartitionsWithIndex(new SparkJob(shell, shell_args, options.jobStdOut, options.jobStdErr), false).collect();
        if (returnCodes.get(0) == 0) {
            logger.info("job success.");
        } else {
            logger.error("job failed with return code: {}", returnCodes.get(0));
        }

        sc.close();
    }
}
