package com.tomtom.aktools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.teleatlas.global.common.ddct.DictionaryAssociation;
import com.teleatlas.global.common.ddct.DictionaryFeature;
import com.teleatlas.global.common.ddct.DictionaryModelStoreFactory;
import com.teleatlas.global.common.ddct.DictionaryProperty;
import com.teleatlas.models.ttom.TTOM;
import com.tomtom.cpu.api.features.Association;
import com.tomtom.cpu.api.features.Attribute;
import com.tomtom.cpu.api.features.Feature;
import com.tomtom.cpu.api.geometry.Coordinate;
import com.tomtom.cpu.api.geometry.EmptyGeometry;
import com.tomtom.cpu.api.geometry.Geometry;
import com.tomtom.cpu.api.geometry.LineString;
import com.tomtom.cpu.coredb.client.filters.FeatureTypeFilter;
import com.tomtom.cpu.coredb.client.filters.ParametersBuilder;
import com.tomtom.cpu.coredb.client.filters.feature.FeatureTypeFilterFactory;
import com.tomtom.cpu.coredb.client.impl.ConnectionInfoImpl;
import com.tomtom.cpu.coredb.client.impl.DataConnectionException;
import com.tomtom.cpu.coredb.client.impl.DataConnectionImpl;
import com.tomtom.cpu.coredb.client.interfaces.*;
import com.tomtom.cpu.coredb.client.modifications.Delta;
import com.tomtom.cpu.coredb.client.modifications.FeatureModification;
import com.tomtom.cpu.coredb.common.dto.basicOperations.FeatureAttributesToSet;
import com.tomtom.cpu.coredb.common.dto.basicOperations.FeatureAttributesToSetBuilder;
import com.tomtom.cpu.coredb.common.query.*;
import com.tomtom.cpu.coredb.common.query.tables.Entities;
import com.tomtom.cpu.coredb.mapdata.ModificationType;
import com.tomtom.cpu.coredb.mutable.MutableObjectManipulator;
import com.tomtom.cpu.coredb.quality.CheckResults;
import com.tomtom.cpu.coredb.writeapi.impl.logical.CascadeEditOptionsImpl;
import com.tomtom.cpu.coredb.writeapi.impl.logical.EditOptionsFactory;
import com.tomtom.cpu.coredb.writeapi.impl.logical.EditOptionsImpl;
import com.tomtom.cpu.coredb.writeapi.impl.logical.NoCascadeStrategy;
import com.tomtom.cpu.coredb.writeapi.logicaltransactions.editoptions.EditOptionName;
import org.assertj.core.api.Java6Assertions;
import org.junit.*;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;
import pl.touk.throwing.ThrowingSupplier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for simple App.
 */
public class CommitingTest
{



    private static final Logger LOGGER = LoggerFactory.getLogger(CommitingTest.class);
    private static final TTOM ttom = new TTOM(DictionaryModelStoreFactory.getModelStore());
    private static DataConnection connection;
    //private static String COREDB_URL = "http://cpp-read.maps-contentops.amiefarm.com:8000/cobaz/coredb-main-ws/";
    //private static String COREDB_URL = "http://172.28.96.159:8080/coredb-main-ws/";
    //private static String COREDB_URL = "http://localhost:8080/coredb-main-ws/";

    //private static String COREDB_URL = "http://172.28.108.89:8080/coredb-main-ws/";
    private static String COREDB_URL = "http://cpp-camudev-tbdtst-corews.maps-contentops.amiefarm.com/coredb-main-ws/";
    //private static String COREDB_URL ="http://cpp-camudev-commit-corews.maps-contentops.amiefarm.com/coredb-main-ws/";

    //private static String COREDB_URL ="http://cpp-camudev-commit-corews.maps-contentops.amiefarm.com/coredb-main-ws/";
    //private static String COREDB_URL = "http://cppedit-corewslrw.maps-india-contentops.amiefarm.com/coredb-main-ws";
    //private static String COREDB_URL = "http://hkm-cpp-r2-coredb-main-ws-live-rw.service.eu-west-1-hkm.maps-hkm.amiefarm.com:8080/coredb-main-ws";
    private ExecutorService pool = Executors.newCachedThreadPool();


    private long commitTotalTimeMS = 0;

    @Rule
    public Timeout globalTimeout= new Timeout(20 * 60 * 1_000);


    @After
    public void afterTest() {
        pool.shutdownNow();
    }

    @BeforeClass
    public static void init() throws DataConnectionException {
        connection = new DataConnectionImpl();

        connection.connect(new ConnectionInfoImpl(COREDB_URL, UUID.randomUUID()));
    }



    @AfterClass
    public static void teardown() throws DataConnectionException {
        connection.disconnect();

    }


    @Test
    public void commitTransactionManytmes() {

        Branch branch = getWrite().createDisconnectedBranch();
        List<ThrowingSupplier<CheckResults, Exception>> commiters = ImmutableList.of(
            createEditFuture(branch, "ORDER_", 0, 0,  0),
            createEditFuture(branch, "ORDER_", 0, 0,  0),
            createEditFuture(branch, "ORDER_", 1, 0,  0),
            createEditFuture(branch, "ORDER_", 1, 0,  0)
        );

        List<CheckResults.STATUS> results = IntStream.range(1, 400)
            .mapToObj(i -> commiters.stream().map(commiter -> pool.submit(() -> commiter.uncheck().get())))
            .flatMap(m -> m)
            .map(f -> ThrowingSupplier.unchecked((() -> f.get())))
            .map(r -> r.get().getCheckStatus())
            .collect(Collectors.toList());
        assertThat(results.stream().filter(s -> s.equals(CheckResults.STATUS.OK)).count()).isEqualTo(1);
    }

    @Test
    public void massiveCommitInOrderTest() throws Throwable {

        int parentOrdersCount = getTestParameter("PARENTS", 20);
        int dependantsCount = getTestParameter("DEPENDANTS", 20);
        int repeat = getTestParameter("REPEAT", 20);



        try {

        IntStream.range(0, repeat).forEach((i)-> {
            try {
                performParalelCommits(i, parentOrdersCount, dependantsCount);
            } catch (DataConnectionException e) {
                e.printStackTrace();
            }
        });
        } finally {
            LOGGER.info("TOTALL_COMMIT_TIME_MS=" + commitTotalTimeMS);
            LOGGER.info("AVG_COMMIT_TIME_MS=" + commitTotalTimeMS/(parentOrdersCount*dependantsCount*repeat));
        }
    }

    private Integer getTestParameter(String paramName, int defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(Integer::parseInt).orElse(defaultValue);
    }

    private void performParalelCommits(int testNo, int rows, int cols) throws DataConnectionException {

            String orderId = "ORDER_" + testNo + "_";
            Branch branch = getWrite().createDisconnectedBranch();

            Optional<CheckResults> anyError = IntStream.range(0, rows)
                .mapToObj(i -> IntStream.range(0, cols)
                    .mapToObj(n -> {
                        return pool.submit(() -> createEditFuture(branch, orderId, cols, i, n));
                    }))
                .flatMap(stream -> stream)
                .collect(Collectors.toList())
                .stream()
                .map(f -> ThrowingSupplier.unchecked(() -> f.get()).get())
                .map(commitAction -> pool.submit(() -> commitAction.uncheck().get()))
                .collect(Collectors.toList())
                .stream()
                .map(f -> (ThrowingSupplier<CheckResults, Exception>)() -> f.get())
                .map(thf -> thf.uncheck().get())
                .filter(Objects::nonNull)
                .filter(result -> result.getCheckStatus() != CheckResults.STATUS.OK)
                .findAny();

            anyError.ifPresent(result -> assertThat(result.getCheckStatus()).isEqualTo(CheckResults.STATUS.OK));

            Version version = connection.getJournalInterface().getCurrentVersion(branch);
            Java6Assertions.assertThat(version.getJournalVersion()).isGreaterThanOrEqualTo(rows * cols);
    }

    private ThrowingSupplier<CheckResults, Exception> createEditFuture(Branch branch, String orderId, int cols, int i, int n) {
        LOGGER.info("Create feature with orderId {} row {} col {} at thread {}" , i, n, Thread.currentThread().getName());
        int shift =  500;
        int x0 = 50000 + i * shift;
        int y0 = 4 + shift * n;
        Coordinate[] coordinates =
            {new Coordinate(x0, y0), new Coordinate(x0 + 1, y0 + 1)};
        return createGeometryInTx(branch, orderId + (i * cols + n), n == 0 ? null : orderId + (i * cols), coordinates);
    }

    private ThrowingSupplier<CheckResults, Exception> createGeometryInTx(Branch branch, String orderId, String dependsOnOrderId, Coordinate[] coordinates) {

        Transaction tx = createTransactonWithRetires(branch, coordinates);

        return () -> {
            return commitWithRetry(orderId, dependsOnOrderId, tx);
        };
    }

    private Transaction createTransactonWithRetires(Branch branch, Coordinate[] coordinates) {
        int retryCounter = 100;
        Throwable laseException = new RuntimeException("Ouh, no way");
        while (retryCounter > 0) {
            try {
                Transaction tx = getWrite().newTransaction(branch);
                createAndGetCreatedFeatures(tx, Arrays.asList(coordinates));
                return tx;
            } catch (Throwable t) {
                laseException = t;
            }
        }
        throw new RuntimeException("Could not create edit even ater 100 retries", laseException);
    }

    private CheckResults commitWithRetry(String orderId, String dependsOnOrderId, Transaction tx) throws CheckException, InterruptedException {
        int retryCounter = 100;
        Throwable laseException = new RuntimeException("Ouh, no way");
        StopWatch s = new StopWatch();
        s.start();
        try {
            while (retryCounter > 0) {
                LOGGER.info("Commit orderId {} depending on {} at thread {}", orderId, dependsOnOrderId, Thread.currentThread().getName());
                try {
                    CheckResults checkResults = getWrite().commitTransactionInOrder(tx, orderId, dependsOnOrderId);
                    //CheckResults checkResults = getWrite().commitTransaction(tx);
                    LOGGER.info("Committed orderId {} depending on {} at thread {}", orderId, dependsOnOrderId, Thread.currentThread().getName());
                    return checkResults;
                } catch (com.tomtom.cpu.coredb.common.service.exception.ConcurrentObjectModificationException comex) {
                    LOGGER.info("Conflict detected, dont' try to commit it any more orderId {} depending on {} at thread {}", orderId, dependsOnOrderId,
                        Thread.currentThread().getName());
                    return null;
                } catch (Throwable x) {
                    LOGGER.info("ERROR while Committed orderId {} depending on {} at thread {}, {}", orderId, dependsOnOrderId,
                        Thread.currentThread().getName(),
                        x);
                    laseException = x;
                    if (x.getMessage().contains("TRANSACTION_ALREADY_COMMITTED")) {
                        LOGGER.info("ERROR already Committed orderId {} depending on {} at thread {}, {}", orderId, dependsOnOrderId,
                            Thread.currentThread().getName(),
                            x);
                        return null;
                    }
                }
                Thread.sleep(5000);
            }
        } finally {
            s.stop();
            commitTotalTimeMS += s.getTotalTimeMillis();
            LOGGER.info("COMMIT TIME: " + s.shortSummary());
        }
        throw new RuntimeException("Could not commit even ater 100 retries", laseException);
    }

    private void createAndGetCreatedFeatures(final Transaction tx, final List<Coordinate> geometry) {

        DictionaryFeature EDGE = ttom.TTOM_Topology.FEATURES.Edge;

        final EditOptionsImpl editOptions = new EditOptionsImpl();
        editOptions.addOption(EditOptionName.CASCADE_EDIT_OPTIONS, new CascadeEditOptionsImpl(new NoCascadeStrategy()));

        final LineString chainCrossingBorder = new LineString(geometry);
        final Delta delta = tx.createGeometry(chainCrossingBorder, EDGE, editOptions, null);

        final FeatureTypeFilter ftfAll = FeatureTypeFilterFactory.createAllFeatureTypesFilter();

    }

    private WriteInterface getWrite()  {

        try {
            return connection.getWriteInterface();
        } catch (DataConnectionException e) {
            throw new RuntimeException("Get write failed", e);
        }
    }

}
