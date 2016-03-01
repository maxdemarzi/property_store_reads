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

    @Param({"10000"})
    public int nodeCount;

    @Param({"12"})
    public int propertyCount;

    @Setup
    public void prepare() throws IOException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        populateDb(db);
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

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(value = 1, jvmArgsAppend = {"-Xms2048m", "-Xmx2048m"})
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRandomNodeReadAllProperties(Blackhole bh) throws IOException {
        Map<String, Object> props;
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(rand.nextInt(nodeCount - 1));
            props = node.getAllProperties();
            tx.success();
        }
        bh.consume(props);
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
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
            props = node.getAllProperties();
            tx.success();
        }
        bh.consume(props);
    }
}
