package com.maxdemarzi;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ReadsBenchmark {

    private GraphDatabaseService db;
    private static final Random rand = new Random();
    String[] properties;

    @Param({"10000"})
    public int nodeCount;

    @Param({"12"})
    public int propertyCount;

    @Setup
    public void prepare() throws IOException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        populateDb(db);
        properties = new String[propertyCount];
        for ( int j = 0; j < propertyCount; j++) {
         properties[j]= "prop" + j;
        }
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
    }

    private void populateDb(GraphDatabaseService db) throws IOException {
        Transaction tx = db.beginTx();
        try {
            for (int i = 0; i < nodeCount; i++) {
                db.createNode();
                // Commit every x transactions
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }
            tx.success();
        } finally {
            tx.close();
        }

        for ( int j = 0; j < propertyCount; j++) {
            tx = db.beginTx();
            try {
                for (int i = 0; i < nodeCount; i++) {
                    Node node = db.getNodeById(i);
                    node.setProperty("prop" + j, rand.nextInt(10000));
                    // Commit every x transactions
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
                tx.success();
            } finally {
                tx.close();
            }
        }
    }

// Uncomment for 2.3.x
//    @Benchmark
//    @Warmup(iterations = 10)
//    @Measurement(iterations = 50)
//    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
//    @Threads(4)
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.SECONDS)
//    public void measureRandomNodeReadAllProperties(Blackhole bh) throws IOException {
//        Map<String, Object> props;
//        try (Transaction tx = db.beginTx()) {
//            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
//            props = node.getAllProperties();
//            tx.success();
//        }
//        bh.consume(props);
//    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 50)
    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRandomNodeReadOnePropertyAtaTime(Blackhole bh) throws IOException {
        Map<String, Object> props = new HashMap<>();
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
            for (String property : node.getPropertyKeys()) {
                props.put(property, node.getProperty(property));
            }
            tx.success();
        }
        bh.consume(props);
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 50)
    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRandomNodeReadOneRandomProperty(Blackhole bh) throws IOException {
        Map<String, Object> props = new HashMap<>();
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
            props.put("property", node.getProperty("prop" + rand.nextInt(propertyCount)));
            tx.success();
        }
        bh.consume(props);
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 50)
    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRandomNodeReadThreeRandomProperty(Blackhole bh) throws IOException {
        Map<String, Object> props = new HashMap<>();
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
            props.put("property1", node.getProperty("prop" + rand.nextInt(propertyCount)));
            props.put("property2", node.getProperty("prop" + rand.nextInt(propertyCount)));
            props.put("property3", node.getProperty("prop" + rand.nextInt(propertyCount)));
            tx.success();
        }
        bh.consume(props);
    }

// Uncomment for 2.3.x
//    @Benchmark
//    @Warmup(iterations = 10)
//    @Measurement(iterations = 50)
//    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
//    @Threads(4)
//    @BenchmarkMode(Mode.Throughput)
//    @OutputTimeUnit(TimeUnit.SECONDS)
//
//    public void measureRandomNodeReadSomeProperty(Blackhole bh) throws IOException {
//        Map<String, Object> props;
//        try (Transaction tx = db.beginTx()) {
//            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
//            props = node.getProperties(properties);
//            tx.success();
//        }
//        bh.consume(props);
//    }
}
