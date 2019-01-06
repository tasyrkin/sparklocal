package com.tasyrkin.sparklocal;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaWordCount {
    public static void main(String[] args) throws InterruptedException, IOException {

        // create Spark context with Spark configuration
        final JavaSparkContext sc =
                new JavaSparkContext(
                        "local",
                        "Spark Count",
                        new SparkConf().setAppName("Spark Count")
                );

        // get threshold
        final int threshold = Integer.parseInt(args[1]);

        //System.out.println(String.format("file to read = [%s], threshold = [%s]", args[0], threshold));

        for (Path file : Files.list(Paths.get(args[0])).collect(Collectors.toList())) {
            if (Files.isRegularFile(file)) {
                // read in text file and split each document into words
                System.out.println(String.format("Examining file [%s]", file));
                try {
                    System.out.println(submitAJob(sc.textFile(file.toString()), threshold));
                } catch (Exception e) {
                    System.out.println("Ups, exception " + e.getMessage());
                }
            }
        }

//        // filter out words with fewer than threshold occurrences
    }

    private static List<Tuple2<Character, Integer>> submitAJob(final JavaRDD<String> stringJavaRDD, int threshold) {
        JavaRDD<String> tokenized = stringJavaRDD.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(final String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(final String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(final Integer i1, final Integer i2) throws Exception {
                        return i1 + i2;
                    }
                }
        );

        JavaPairRDD<String, Integer> filtered = counts.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(final Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2._2 >= threshold;
                    }
                }
        );

        // count characters
        JavaPairRDD<Character, Integer> charCounts = filtered.flatMap(
                new FlatMapFunction<Tuple2<String, Integer>, Character>() {
                    @Override
                    public Iterator<Character> call(final Tuple2<String, Integer> s) throws Exception {
                        Collection<Character> chars = new ArrayList<Character>(s._1().length());
                        for (char c : s._1().toCharArray()) {
                            chars.add(c);
                        }
                        return chars.iterator();
                    }
                }
        ).mapToPair(
                new PairFunction<Character, Character, Integer>() {
                    @Override
                    public Tuple2<Character, Integer> call(Character c) {
                        return new Tuple2<Character, Integer>(c, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        return charCounts.collect();
    }
}
