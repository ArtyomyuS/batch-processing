package com.adswizz.ds;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A simple spark job to submit.
 */
public class JavaSimpleJob
{

    public static final String JOB_NAME = "java-simple-word-count-job";
    public static final String MASTER_SERVER = "local[2]";
    private static final Pattern SPACE = Pattern.compile(" ");

    /** Job arguments */
    enum Attribute{
        File;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    public static void main(String[] args)
    {
        final Namespace namespace = parseArguments(args);
        final String filePath = namespace.get(Attribute.File.toString());

        final SparkConf sparkConf = new SparkConf().setAppName(JOB_NAME).setMaster(MASTER_SERVER);
        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile(filePath, 1);
        JavaRDD<String> words = lines.flatMap((s) -> Arrays.asList(SPACE.split(s)));
        JavaPairRDD<String, Integer> ones = words.mapToPair((s) -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        jsc.stop();
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser(JOB_NAME)
            .defaultHelp(true)
            .description("Calculate how many times a word was found in the file.");
        parser.addArgument("-f", "--file")
            .required(true)
            .help("Specify file to process");
        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        return namespace;
    }
}
