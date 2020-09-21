package com.tomtom.aktools;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import pl.touk.throwing.ThrowingRunnable;

import java.sql.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class BigQueryTest {

    private static final int FEATURES = ParamReader.getTestParameter("FEATURES_PER_QUERY", 100);
    private static final int REPEAT = ParamReader.getTestParameter("REPEAT", 10);
    private String bigQueryWithBindings;
    private String bigQueryWithUnnestAndBindings;

    private static String VMDS_JDBC_URL = ParamReader.getTestParameter("VMDS_JDBC_URL", "jdbc:postgresql://172.29.20.123/cpp");
    private static String VMDS_DB_USER = ParamReader.getTestParameter("VMDS_DB_USER", "cpp");
    private static String VMDS_DB_PASSWORD = ParamReader.getTestParameter("VMDS_DB_PASSWORD", "cpp");

    class BVR {

        public BVR(String branch, int verFrom, int verTo) {

            this.branch = UUID.fromString(branch);
            this.verFrom = verFrom;
            this.verTo = verTo;
        }

        UUID branch;
        int verFrom;
        int verTo;
    }

    final List<BVR> bvrs = ImmutableList.of(
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

    final String brancSel = "("
        + "(branch = '3296e81d-c113-4f3b-9316-89aaaf6ad5a6'::uuid) AND (version > '1915'::bigint) AND (version <= '1920'::bigint)) OR\n"
        + "((branch = '97e9ce99-a704-48a9-9509-8954235e78d4'::uuid) AND (version > '3944'::bigint) AND (version <= '3947'::bigint)) OR\n"
        + "((branch = '74434384-bebf-46b1-be28-356eb5afad6b'::uuid) AND (version > '8607'::bigint) AND (version <= '8610'::bigint)) OR\n"
        + "((branch = 'bb5f2f18-089a-40d8-8e55-83d1883eadf3'::uuid) AND (version > '15672'::bigint) AND (version <= '15675'::bigint)) OR\n"
        + "((branch = 'd5e9dbf4-e38f-436b-a22c-cedddd0f74a5'::uuid) AND (version > '15726'::bigint) AND (version <= '15729'::bigint)) OR\n"
        + "((branch = '4bd6acc5-06dc-4953-b1ac-6d541ab8eaab'::uuid) AND (version > '21640'::bigint) AND (version <= '21643'::bigint)) OR\n"
        + "((branch = '69d35f44-0fa5-4623-8964-f3a5231ffe65'::uuid) AND (version > '24241'::bigint) AND (version <= '24250'::bigint)) OR\n"
        + "((branch = '8e15ef8d-8d07-4dd5-a6d8-5fc5b241c68b'::uuid) AND (version > '34917'::bigint) AND (version <= '34920'::bigint)) OR\n"
        + "((branch = '04035f45-46c4-4211-9024-25478b5cc9ca'::uuid) AND (version > '38785'::bigint) AND (version <= '38788'::bigint)) OR\n"
        + "((branch = '12f5a9a3-bee2-4db0-b514-756dd00c9647'::uuid) AND (version > '38943'::bigint) AND (version <= '38946'::bigint)) OR\n"
        + "((branch = 'efbb0a09-5198-49ac-8492-45c7b9e2ce00'::uuid) AND (version > '49355'::bigint) AND (version <= '49358'::bigint)) OR\n"
        + "((branch = '6e9668f2-716f-4742-9255-93566fbbf4ec'::uuid) AND (version > '54320'::bigint) AND (version <= '54323'::bigint)) OR\n"
        + "((branch = 'c5f71617-a995-47a2-93ad-748982eed0a0'::uuid) AND (version > '93193'::bigint) AND (version <= '93201'::bigint)) OR\n"
        + "((branch = '406177f1-8e2f-4e41-b1e8-420a26b3e940'::uuid) AND (version > '94579'::bigint) AND (version <= '94582'::bigint)) OR\n"
        + "((branch = 'b24451ab-d5e5-4d77-80b2-1160f14fdc97'::uuid) AND (version > '95275'::bigint) AND (version <= '95282'::bigint)) OR\n"
        + "((branch = 'aedd6e84-121f-4ac8-b037-196d8062dbfb'::uuid) AND (version > '101330'::bigint) AND (version <= '101336'::bigint)) OR\n"
        + "((branch = 'c1c024b5-d5b2-41e2-9d83-a001125e62f7'::uuid) AND (version > '108705'::bigint) AND (version <= '108713'::bigint))";


;
    final String queryBase = "with data as (values $VALUES)\n"
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
        + "union\n"
        + " select\n"
        + "  fpe.feature_id, fpe.branch, fpe.version\n"
        + " from\n"
        + "  vmds_r2.feature_property_entry fpe\n"
        + " where\n"
        + "  ($BVRSELECTOR)"
        + ") as subq\n\n";
    private Connection db;
    private String bigQueryWithoutBindings;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        this.getClass().getClassLoader().loadClass("org.postgresql.jdbc.PgConnection");
        db = DriverManager.getConnection(VMDS_JDBC_URL, VMDS_DB_USER, VMDS_DB_PASSWORD);

        String readyUUIDS = IntStream.range(0, 2)
            .mapToObj(i -> UUID.randomUUID())
            .map(uuid -> "('" + uuid.toString() + "')")
            .collect(Collectors.joining(", "));

        bigQueryWithoutBindings = "/*NOBIND*/" + queryBase;
        bigQueryWithoutBindings = bigQueryWithoutBindings.replaceAll("\\$BVRSELECTOR", brancSel);
        bigQueryWithoutBindings = bigQueryWithoutBindings.replaceAll("\\$VALUES", readyUUIDS);



        // query with bindings
        String bvrSelectorWithBindings = IntStream.range(0, bvrs.size())
            .mapToObj(i -> "((branch = ?::uuid) AND (version > ?::bigint) AND (version <= ?::bigint))")
            .collect(Collectors.joining(" OR "));
        String featuresSelectorWithBinding = IntStream.range(0, FEATURES)
            .mapToObj(i -> "(?)")
            .collect(Collectors.joining(", "));

        bigQueryWithBindings = "/*WIBIND*/" + queryBase;
        bigQueryWithBindings = bigQueryWithBindings.replaceAll("\\$BVRSELECTOR", bvrSelectorWithBindings);
        bigQueryWithBindings = bigQueryWithBindings.replaceAll("\\$VALUES", featuresSelectorWithBinding);

        bigQueryWithUnnestAndBindings = "/*WIBIND*/" + queryBase;
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("\\$BVRSELECTOR", bvrSelectorWithBindings);
        bigQueryWithUnnestAndBindings = bigQueryWithUnnestAndBindings.replaceAll("values \\$VALUES", "select unnest(?) fid");

        // dump
        System.out.println("[QUERY]: NOOO_BIND: " + bigQueryWithoutBindings);
        System.out.println("[QUERY]: WITH_BIND: " + bigQueryWithBindings);
    }


    @Test
    public void run_selec1() throws SQLException {


        Statement statement = db.createStatement();

        ResultSet res = statement.executeQuery("SELECT 1");
        assertThat(res.next()).isTrue();
        assertThat(res.getInt(1)).isEqualTo(1);
        res.close();
    }

    @Test
    public void run_bigQueryInlined() throws SQLException {

        IntStream.range(0, REPEAT)
            .forEach(i -> {
                ThrowingRunnable.unchecked(()-> runInlinedQuery()).run();
            });
    }

    @Test
    public void test_preapredStatementWithBinding() {

        IntStream.range(0, REPEAT)
            .forEach(i -> {
                ThrowingRunnable.unchecked(()-> runPreparedStatement()).run();
            });

    }

    @Test
    public void test_preapredStatementWithUnnest() {

        IntStream.range(0, REPEAT)
            .forEach(i -> {
                ThrowingRunnable.unchecked(()-> runPreparedStatementWithUnnest()).run();
            });

    }

    private void runInlinedQuery() throws SQLException {

        Statement statement = db.createStatement();
        ResultSet res = statement.executeQuery(bigQueryWithoutBindings);
        res.next();
        res.close();
    }


    private void runPreparedStatement() throws SQLException {

        PreparedStatement statement = db.prepareStatement(bigQueryWithBindings);
        AtomicInteger n = new AtomicInteger(1);

        IntStream.range(0, FEATURES)
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> statement.setString(n.incrementAndGet() - 1, UUID.randomUUID().toString())))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
            }))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
            }))
            .forEach(t -> t.run());

        System.out.println("[INFO]: bound " + n.get() + " parameters");

        ResultSet res = statement.executeQuery();
        res.next();
        res.close();
    }


    private void runPreparedStatementWithUnnest() throws SQLException {

        PreparedStatement statement = db.prepareStatement(bigQueryWithUnnestAndBindings);
        AtomicInteger n = new AtomicInteger(1);

        Array featureIds = db.createArrayOf("text", IntStream.range(0, FEATURES).mapToObj(i -> UUID.randomUUID().toString()).toArray());
        statement.setArray(n.incrementAndGet() - 1, featureIds);

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
            }))
            .forEach(t -> t.run());

        IntStream.range(0, bvrs.size())
            .mapToObj(i -> ThrowingRunnable.unchecked(() -> {
                statement.setString(n.incrementAndGet() - 1, bvrs.get(i).branch.toString());
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
                statement.setInt(n.incrementAndGet() - 1, bvrs.get(i).verFrom);
            }))
            .forEach(t -> t.run());

        System.out.println("[INFO]: bound " + n.get() + " parameters");

        ResultSet res = statement.executeQuery();
        res.next();
        res.close();
    }
}
