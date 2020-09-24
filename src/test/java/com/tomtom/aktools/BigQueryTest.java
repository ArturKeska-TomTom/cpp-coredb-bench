package com.tomtom.aktools;

import com.google.common.collect.ImmutableList;
import com.tomtom.cpu.coredb.id.index.BranchVersion;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import pl.touk.throwing.ThrowingRunnable;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class BigQueryTest {

    private static final int FEATURES = ParamReader.getTestParameter("FEATURES_PER_QUERY", 10);
    private static final int REPEAT = ParamReader.getTestParameter("REPEAT", 1);

    private static String VMDS_JDBC_URL = ParamReader.getTestParameter("VMDS_JDBC_URL", "jdbc:postgresql://172.29.20.123/cpp");
    private static String VMDS_DB_USER = ParamReader.getTestParameter("VMDS_DB_USER", "cpp");
    private static String VMDS_DB_PASSWORD = ParamReader.getTestParameter("VMDS_DB_PASSWORD", "cpp");

    private static String CORESUP_JDBC_URL = ParamReader.getTestParameter("CORESUP_JDBC_URL", "jdbc:postgresql://172.29.23.110/cpp");
    private static String CORESUP_DB_USER = ParamReader.getTestParameter("CORESUP_DB_USER", "cpp");
    private static String CORESUP_DB_PASSWORD = ParamReader.getTestParameter("CORESUP_DB_PASSWORD", "cpp");

    private static int SMALL_BRANCHES_COUNT = ParamReader.getTestParameter("SMALL_BRANCHES_COUNT", 1);
    private static int BIG_BRANCHES_COUNT = ParamReader.getTestParameter("BIG_BRANCHES_COUNT", 1);

    private static String CONNECTION_RESET_QUERY  = ParamReader.getTestParameter("CONNECTION_RESET_QUERY", "SELECT 0");
    private static boolean CLOSE_STATEMENTS = ParamReader.getTestParameter("CONNECTION_RESET_QUERY", false);

    private static String SINGLE_QUERY_TEMPLATE = ParamReader.getTestParameter("SINGLE_QUERY_TEMPLATE", "with data as (values $VALUES)\n"
        + "select\n"
        + " feature_id,\n"
        + " branch,\n"
        + " version\n"
        + "from\n"
        + " (\n"
        + " select\n"
        + "  f.id as feature_id, f.branch, f.version\n"
        + " from\n"
        + "  vmds_r2.feature f\n"
        + " where\n"
        + "  ($BVRSELECTOR)"
        + " AND\n"
        + " f.id IN (SELECT CAST(data.column1 AS UUID) FROM DATA) \n"
        + " union\n"
        + " select\n"
        + "  fpe.feature_id, fpe.branch, fpe.version\n"
        + " from\n"
        + "  vmds_r2.feature_property_entry fpe\n"
        + " where\n"
        + "  ($BVRSELECTOR)"
        + " AND\n"
        + " fpe.feature_id IN (SELECT CAST(data.column1 AS UUID) FROM DATA)"
        + ") as subq\n\n");

    private static String SEPARATED_BRANCHES_BY_SIZE_QUERY_TEMPLATE = ParamReader.getTestParameter("SINGLE_QUERY_TEMPLATE", "with data as (values $VALUES)\n"
        + "select feature_id, branch, version\n"
        + "from\n"
        + " (\n"
        + " select\n"
        + "  f.id as feature_id, f.branch, f.version\n"
        + " from\n"
        + "  vmds_r2.feature f\n"
        + " where\n"
        + "  ($BVRSELECTOR_BIG)"
        + " AND\n"
        + " f.id IN (SELECT CAST(data.column1 AS UUID) FROM DATA) \n"

        + " union\n"

        + " select\n"
        + "  f.id as feature_id, f.branch, f.version\n"
        + " from\n"
        + "  vmds_r2.feature f\n"
        + " where\n"
        + "  ($BVRSELECTOR_SMALL)"
        + " AND\n"
        + " f.id IN (SELECT CAST(data.column1 AS UUID) FROM DATA)  \n"

        + " union\n"

        + " select\n"
        + "  fpe.feature_id, fpe.branch, fpe.version\n"
        + " from\n"
        + "  vmds_r2.feature_property_entry fpe\n"
        + " where\n"
        + "  ($BVRSELECTOR_BIG)"
        + " AND\n"
        + " fpe.feature_id IN (SELECT CAST(data.column1 AS UUID) FROM DATA) "
        + ""

        + " union\n"

        + " select\n"
        + "  fpe.feature_id, fpe.branch, fpe.version\n"
        + " from\n"
        + "  vmds_r2.feature_property_entry fpe\n"
        + " where\n"
        + "  ($BVRSELECTOR_SMALL)"
        + " AND\n"
        + " fpe.feature_id IN (SELECT CAST(data.column1 AS UUID) FROM DATA) "

        + ") as sq\n\n");

    class BVR {

        public BVR(UUID branch, long verFrom, long verTo) {

            this.branch = branch;
            this.verFrom = verFrom;
            this.verTo = verTo;
        }

        public BVR(String branch, long verFrom, long verTo) {

            this.branch = UUID.fromString(branch);
            this.verFrom = verFrom;
            this.verTo = verTo;
        }

        UUID branch;
        long verFrom;
        long verTo;
    }

    @Rule
    public ExternalResource performanceLogger = new ExternalResource() {

        @Override
        protected void before() throws Throwable {}

        @Override public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base, Description description) {

            return new org.junit.runners.model.Statement() {
                @Override public void evaluate() throws Throwable {
                    for (int i = 0; i < REPEAT; i++) {
                        base.evaluate();
                    }
                }
            };
        }

        @Override
        protected void after() {}
    };

    List<BVR> bvrs = ImmutableList.of(
        new BVR("3296e81d-c113-4f3b-9316-89aaaf6ad5a6",1915,1920),
        new BVR("97e9ce99-a704-48a9-9509-8954235e78d4",3944,3947),
        new BVR("74434384-bebf-46b1-be28-356eb5afad6b",8607,8610),
        new BVR("bb5f2f18-089a-40d8-8e55-83d1883eadf3",15672,15675),
        new BVR("d5e9dbf4-e38f-436b-a22c-cedddd0f74a5",15726,15729),
        new BVR("4bd6acc5-06dc-4953-b1ac-6d541ab8eaab",21640,21643),
        new BVR("69d35f44-0fa5-4623-8964-f3a5231ffe65",24241,24250),
        new BVR("8e15ef8d-8d07-4dd5-a6d8-5fc5b241c68b",34917,34920),
        new BVR("04035f45-46c4-4211-9024-25478b5cc9ca",38785,38788),
        new BVR("12f5a9a3-bee2-4db0-b514-756dd00c9647",38943,38946),
        new BVR("efbb0a09-5198-49ac-8492-45c7b9e2ce00",49355,49358),
        new BVR("6e9668f2-716f-4742-9255-93566fbbf4ec",54320,54323),
        new BVR("c5f71617-a995-47a2-93ad-748982eed0a0",93193,93201),
        new BVR("406177f1-8e2f-4e41-b1e8-420a26b3e940",94579,94582),
        new BVR("b24451ab-d5e5-4d77-80b2-1160f14fdc97",95275,95282),
        new BVR("aedd6e84-121f-4ac8-b037-196d8062dbfb",101330,101336),
        new BVR("c1c024b5-d5b2-41e2-9d83-a001125e62f7",108705,108713));

    private List<BVR> bigBVRS;
    private List<BVR> smallBVRS;


    final String singleQueryBase = "explain analyze " + SINGLE_QUERY_TEMPLATE;
    final String separateBrancehsQueryBase = "explain analyze " + SEPARATED_BRANCHES_BY_SIZE_QUERY_TEMPLATE;
    private Connection vmdsConnection;
    private Connection coresupConnection;

    @Before
    public void init() throws ClassNotFoundException, SQLException {

        this.getClass().getClassLoader().loadClass("org.postgresql.jdbc.PgConnection");
        vmdsConnection = DriverManager.getConnection(VMDS_JDBC_URL, VMDS_DB_USER, VMDS_DB_PASSWORD);
        coresupConnection = DriverManager.getConnection(CORESUP_JDBC_URL, CORESUP_DB_USER, CORESUP_DB_PASSWORD);

        bigBVRS = bvrProbe(true, BIG_BRANCHES_COUNT);
        smallBVRS = bvrProbe(false, SMALL_BRANCHES_COUNT);

        bvrs = Lists.newArrayList(bigBVRS);
        bvrs.addAll(smallBVRS);
    }

    @After
    public void after() throws SQLException {
    }


    @Test
    public void run_selec1() throws SQLException {

        execSelect1(vmdsConnection);
        execSelect1(coresupConnection);
        return;
    }

    @Test
    public void run_bigQueryInlined() throws SQLException {

        runInlinedQuery();
    }

    @Test
    public void test_preapredStatementWithBinding() throws SQLException {

        runPreparedStatement();
    }

    @Test
    public void test_preapredStatementWithUnnest() throws SQLException {

        runPreparedStatementWithUnnest();
    }


    @Test
    public void test_runPreparedStatementWithUnnest_SeparateBigAndSmallBrances() throws SQLException {
        runPreparedStatementWithUnnest_SeparateBigAndSmallBrances();
    }

    @Test
    public void test_loadBranchesWithSpecifiedBranchStructure() throws SQLException {

        assertThat(bvrs).hasSize(SMALL_BRANCHES_COUNT + BIG_BRANCHES_COUNT);
    }



    private List<BVR> bvrProbe(boolean bigBranches, int count) throws SQLException {

        String query =
            "select branch, jsq.version from branch_stats bs join journal_r2.journalbranchversionseq jsq on jsq.branch_id=bs.branch::text where random()>0.2 order by size $ORDER limit ?"
                .replaceAll("\\$ORDER", bigBranches ? "desc" : "asc");
        System.out.println(query);
        PreparedStatement statement = vmdsConnection.prepareStatement(
            query);

        statement.setInt(1, count);

        ResultSet dbResult = statement.executeQuery();
        List<BranchVersion> bvs = Lists.newArrayList();
        while (dbResult.next()) {
            UUID branchUUID = UUID.fromString(dbResult.getString(1));
            long toVersion = dbResult.getLong(2);
                bvs.add(new BranchVersion(branchUUID, toVersion));
        }

        dbResult.close();
        reset();
        return getBranchFromVersions(bvs);
    }

    private List<BVR> getBranchFromVersions(List<BranchVersion> bvs) throws SQLException {

        PreparedStatement statement = coresupConnection.prepareStatement("with b as (select unnest(ARRAY[?])::uuid branch) "
            + "select source_version, branch from branches_r2.branches_information bi join b as bb on bb.branch=bi.branch_uuid");
        try {
            Array featureIds = vmdsConnection.createArrayOf("text", bvs.stream().map(b -> b.getBranchId()).toArray());
            statement.setArray(1, featureIds);
            ResultSet dbRes = statement.executeQuery();
            Map<String, Long> fromVersions = new HashMap<>();
            while (dbRes.next()) {
                long toVer = dbRes.getLong(1);
                String branch = dbRes.getString(2);
                fromVersions.put(branch, toVer);
            }

            return bvs.stream().map(bv -> new BVR(bv.getBranchId(), fromVersions.get(bv.getBranchId().toString()), bv.getVersion())).collect(Collectors.toList());
        } finally {
            statement.close();
        }

    }

    private void runInlinedQuery() throws SQLException {

        String readyUUIDS = IntStream.range(0, FEATURES)
            .mapToObj(i -> UUID.randomUUID())
            .map(uuid -> "('" + uuid.toString() + "')")
            .collect(Collectors.joining(", "));

        final String brancSel = createBranchSelector(bvrs);

        String bigQueryWithoutBindings = "/*NOBIND*/" + singleQueryBase;
        bigQueryWithoutBindings = bigQueryWithoutBindings.replaceAll("\\$BVRSELECTOR", brancSel);
        bigQueryWithoutBindings = bigQueryWithoutBindings.replaceAll("\\$VALUES", readyUUIDS);

        System.out.println("[INFO]: " + bigQueryWithoutBindings);

        Statement statement = vmdsConnection.createStatement();
        ResultSet res = statement.executeQuery(bigQueryWithoutBindings);
        res.next();
        showExplain(res, "runInlinedQuery");
        res.close();
        reset();
        closeStatementConditionaly(statement);
    }


    private void execSelect1(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery("SELECT 1");
        assertThat(res.next()).isTrue();
        assertThat(res.getInt(1)).isEqualTo(1);
        res.close();
    }


    private void runPreparedStatement() throws SQLException {

        String bvrSelectorWithBindings = IntStream.range(0, bvrs.size())
            .mapToObj(i -> "((branch = ?::uuid) AND (version > ?::bigint) AND (version <= ?::bigint))")
            .collect(Collectors.joining(" OR "));
        String featuresSelectorWithBinding = IntStream.range(0, FEATURES)
            .mapToObj(i -> "(?)")
            .collect(Collectors.joining(", "));

        String bigQueryWithBindings = "/*WIBIND*/" + singleQueryBase;
        bigQueryWithBindings = bigQueryWithBindings.replaceAll("\\$BVRSELECTOR", bvrSelectorWithBindings);
        bigQueryWithBindings = bigQueryWithBindings.replaceAll("\\$VALUES", featuresSelectorWithBinding);


        PreparedStatement statement = vmdsConnection.prepareStatement(bigQueryWithBindings);
        AtomicInteger n = new AtomicInteger(1);

        IntStream.range(0, FEATURES)
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> statement.setString(n.incrementAndGet() - 1, UUID.randomUUID().toString())))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verTo);
            }))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verTo);
            }))
            .forEach(t -> t.run());

        System.out.println("[INFO]: " + bigQueryWithBindings);
        System.out.println("[INFO]: bound " + n.get() + " parameters");

        ResultSet res = statement.executeQuery();
        res.next();
        showExplain(res, "runPreparedStatement");
        res.close();
        reset();
        closeStatementConditionaly(statement);
    }


    private void runPreparedStatementWithUnnest() throws SQLException {

        String bvrSelectorWithBindings = IntStream.range(0, bvrs.size())
            .mapToObj(i -> "((branch = ?::uuid) AND (version > ?::bigint) AND (version <= ?::bigint))")
            .collect(Collectors.joining(" OR "));


        String bigQueryWithUnnestAndBindings = "/*WIBIND*/" + singleQueryBase;
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("\\$BVRSELECTOR", bvrSelectorWithBindings);
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("values \\$VALUES", "select unnest(?) column1");

        PreparedStatement statement = vmdsConnection.prepareStatement(bigQueryWithUnnestAndBindings);
        AtomicInteger n = new AtomicInteger(1);

        Array featureIds = vmdsConnection.createArrayOf("text", IntStream.range(0, FEATURES).mapToObj(i -> UUID.randomUUID().toString()).toArray());
        statement.setArray(n.incrementAndGet() - 1, featureIds);

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verTo);
            }))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verTo);
            }))
            .forEach(t -> t.run());

        System.out.println("[INFO]: " + bigQueryWithUnnestAndBindings);
        System.out.println("[INFO]: bound " + n.get() + " parameters");

        ResultSet res = statement.executeQuery();
        res.next();
        showExplain(res, "runPreparedStatementWithUnnest");
        res.close();
        reset();
    }



    private void runPreparedStatementWithUnnest_SeparateBigAndSmallBrances() throws SQLException {

        String bvrSelectorWithBindings = IntStream.range(0, bvrs.size())
            .mapToObj(i -> "((branch = ?::uuid) AND (version > ?::bigint) AND (version <= ?::bigint))")
            .collect(Collectors.joining(" OR "));


        String bigQueryWithUnnestAndBindings = "/*WIBIND-SmallBig*/" + separateBrancehsQueryBase;
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("\\$BVRSELECTOR_BIG", bvrSelectorWithBindings);
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("\\$BVRSELECTOR_SMALL", bvrSelectorWithBindings);
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("values \\$VALUES", "select unnest(?) column1");

        PreparedStatement statement = vmdsConnection.prepareStatement(bigQueryWithUnnestAndBindings);
        AtomicInteger n = new AtomicInteger(1);

        Array featureIds = vmdsConnection.createArrayOf("text", IntStream.range(0, FEATURES).mapToObj(i -> UUID.randomUUID().toString()).toArray());
        statement.setArray(n.incrementAndGet() - 1, featureIds);

        for (int p = 0; p < 4; p ++) {
            IntStream.range(0, bvrs.size())
                .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                    statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                    statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                    statement.setLong(n.incrementAndGet() - 1, bvrs.get(i).verTo);
                }))
                .forEach(t -> t.run());
        }

        System.out.println("[INFO]: " + bigQueryWithUnnestAndBindings);
        System.out.println("[INFO]: bound " + n.get() + " parameters");

        ResultSet res = statement.executeQuery();
        res.next();
        showExplain(res, "runPreparedStatementWithUnnest_SeparateBigAndSmallBrances");
        res.close();
        reset();
    }

    private void reset() throws SQLException {
        Statement statement = vmdsConnection.createStatement();
        statement.execute(CONNECTION_RESET_QUERY);
        statement.close();
    }

    private void closeStatementConditionaly(Statement statement) throws SQLException {
        if (CLOSE_STATEMENTS) {
            statement.close();
        }
    }

    private static String createBranchSelector(List<BVR> bvrs) {
        return "(" +
            bvrs.stream()
                .map(bvr -> "((branch = '$BRANCH'::uuid) AND (version > '$FROM'::bigint) AND (version <= '$TO'::bigint))"
                    .replaceAll("\\$BRANCH", bvr.branch.toString())
                    .replaceAll("\\$FROM", "" + bvr.verFrom)
                    .replaceAll("\\$TO", "" + bvr.verTo)
                )
                .collect(Collectors.joining("OR"))
            + ")";
    };

    private static void showExplain(ResultSet resultSet, String tag) throws SQLException {

        System.out.println("EXPLAIN ANALYZE /*TAG*/:\n".replaceAll("TAG", tag));
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
