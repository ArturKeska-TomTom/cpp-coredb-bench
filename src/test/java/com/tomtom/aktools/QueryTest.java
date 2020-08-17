package com.tomtom.aktools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.teleatlas.global.common.ddct.*;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.throwing.ThrowingSupplier;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Unit test for simple App.
 */
public class QueryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTest.class);
    private static final TTOM ttom = new TTOM(DictionaryModelStoreFactory.getModelStore());
    private static DictionaryAssociation[] specificPartOfComplexAdministrativeAreaAssociations;
    private static DictionaryProperty[] specificFeatureWithMandatoryOfficialCodeProperties;
    private static DataConnection connection;
    //private static String COREDB_URL = "http://cpp-read.maps-contentops.amiefarm.com:8000/cobaz/coredb-main-ws/";
    private static String COREDB_URL = "http://172.28.96.159:8080/coredb-main-ws/";
    //private static String COREDB_URL = "http://cpp-camudev-tbdtst-corews.maps-contentops.amiefarm.com/coredb-main-ws/";

    //private static String COREDB_URL ="http://cpp-camudev-commit-corews.maps-contentops.amiefarm.com/coredb-main-ws/";
    //private static String COREDB_URL = "http://cppedit-corewslrw.maps-india-contentops.amiefarm.com/coredb-main-ws";
    //private static String COREDB_URL = "http://hkm-cpp-r2-coredb-main-ws-live-rw.service.eu-west-1-hkm.maps-hkm.amiefarm.com:8080/coredb-main-ws";
    private static String BRANCH = "233b38a4-f0bf-4289-bfdc-7f2a04fc4ab3";


    @BeforeClass
    public static void init() throws DataConnectionException {
        specificPartOfComplexAdministrativeAreaAssociations = new DictionaryAssociation[]{
                ttom.TTOM_Core.ASSOCS.Order1AreaOfCountry,
                ttom.TTOM_Core.ASSOCS.Order2AreaOfOrder1Area,
                ttom.TTOM_Core.ASSOCS.Order3AreaOfOrder2Area,
                ttom.TTOM_Core.ASSOCS.Order4AreaOfOrder3Area,
                ttom.TTOM_Core.ASSOCS.Order5AreaOfOrder4Area,
                ttom.TTOM_Core.ASSOCS.Order6AreaOfOrder5Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfCountry,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder1Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder2Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder3Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder4Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder5Area,
                ttom.TTOM_Core.ASSOCS.Order7AreaOfOrder6Area,
                ttom.TTOM_Core.ASSOCS.Order8AreaOfCountry,
                ttom.TTOM_Core.ASSOCS.Order8AreaOfOrder1Area,
                ttom.TTOM_Core.ASSOCS.Order8AreaOfOrder7Area,
                ttom.TTOM_Core.ASSOCS.Order9AreaOfOrder8Area
        };

        specificFeatureWithMandatoryOfficialCodeProperties = new DictionaryProperty[]{
                ttom.TTOM_Core.FEATURES.Order1Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order2Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order3Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order4Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order5Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order6Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order7Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order8Area.OfficialCode,
                ttom.TTOM_Core.FEATURES.Order9Area.OfficialCode
        };

        connection = new DataConnectionImpl();

        connection.connect(new ConnectionInfoImpl(COREDB_URL, UUID.randomUUID()));
    }



    @AfterClass
    public static void teardown() throws DataConnectionException {
        connection.disconnect();
    }

    private UUID getCountryUUID(final String countryCode, final MapView mapView) {

        final SelectConditionStep<Record2<UUID, DictionaryFeature>> query =
                Q.select(Entities.FEATURE.ID, Entities.FEATURE.TYPE) //
                        .from(Entities.FEATURE)//
                        .join(Entities.ATTRIBUTE).on(Entities.FEATURE.ID.eq(Entities.ATTRIBUTE.PARENT_ID))
                        .where(Entities.FEATURE.TYPE.in(ttom.TTOM_Core.FEATURES.Country)
                                .and(Entities.ATTRIBUTE.TYPE.eq(ttom.TTOM_Core.FEATURES.Country.ISOCountryCode))
                                .and(Entities.ATTRIBUTE.VALUE.eq(countryCode)));
        final Result<Record2<UUID, DictionaryFeature>> record2s = mapView.fetch(query);
        final Iterator<Record2<UUID, DictionaryFeature>> iterator = record2s.iterator();
        if (iterator.hasNext())
            return iterator.next().value1();
        throw new RuntimeException("failed to get feature for ISOCountryCode " + countryCode);
    }

    private UUID getAdminAreaUUID(UUID parentId, String areaCode, MapView mapView) {
        SelectConditionStep<Record1<UUID>> query =
                Q.select(Entities.ASSOCIATION.TARGET_FEATURE_ID) //
                        .from(Entities.ASSOCIATION) //
                        .join(Entities.ATTRIBUTE).on(Entities.ATTRIBUTE.PARENT_ID.eq(Entities.ASSOCIATION.TARGET_FEATURE_ID))
                        .where(Entities.ASSOCIATION.SOURCE_FEATURE_ID.eq(parentId)
                                .and(Entities.ASSOCIATION.TYPE.in(specificPartOfComplexAdministrativeAreaAssociations))
                                .and(Entities.ATTRIBUTE.TYPE.in(specificFeatureWithMandatoryOfficialCodeProperties))
                                .and(Entities.ATTRIBUTE.VALUE.eq(areaCode)));
        Result<Record1<UUID>> record1s = mapView.fetch(query);
        Iterator<Record1<UUID>> iterator = record1s.iterator();
        if (iterator.hasNext())
            return iterator.next().value1();

        throw new RuntimeException("failed to get feature for OfficialCode " + areaCode);
    }


    private static final Coordinate C1 = new Coordinate(37170090, 510568220);
    private static final Coordinate C2 = new Coordinate(37172720, 510568540);

    @Test
    public void commitTransactionInParallel() throws DataConnectionException, CheckException, InterruptedException {

        final ExecutorService poll = Executors.newCachedThreadPool();

        List<Callable<CheckResults>> actions = ImmutableList.of(
            "e8ba72c9-37d2-4ec5-8ca2-b62c1e73e796",
            "433fdb4f-40c5-4cce-b020-95402a9e64ff",
            "2f2ff694-d33e-4bcd-80f7-b724ac64c657",
            "af3ef9ab-4f38-4e4e-801b-e3cb36980025",
            "7838fef2-126c-4bf7-beae-82cf181c8af7",
            "f8a81bcb-350f-491c-a8c2-4d68965426dc",
            "bf78f04d-b5d9-4680-b16f-8d33d0026b47",
            "868f1e1a-84f5-47e7-ad12-e5865e550e9a",
            "9af45be3-8af6-42aa-9ad5-8058f23eba52",
            "11b3c0ef-c7c2-4973-8468-890b07ab5a8f"
        )
            .stream()
            .map(UUID::fromString)
            .map(uuid -> (Callable<CheckResults>)() -> {
                WriteInterface write = connection.getWriteInterface();
                Transaction tx = write.reopenTransaction(uuid.toString());
                return write.commitTransactionInOrder(tx, uuid.toString(), "4b88536f-1c77-46e3-8b6f-85312a1b4c74");
            })
            .collect(Collectors.toList());
        poll.invokeAll(actions).stream().forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

    }

    @Test
    public void commitTransaction() throws DataConnectionException, CheckException, InterruptedException {

        final ExecutorService poll = Executors.newCachedThreadPool();

        String txId = "5a8d6929-84f6-44d7-bb0c-19c0fd9dceb9";
            String orderId =  "5a8d6929-84f6-44d7-bb0c-19c0fd9dceb9";
            String dependsOnId = null;

                WriteInterface write = connection.getWriteInterface();
                Transaction tx = write.reopenTransaction(txId);
        CheckResults commitResult = write.commitTransactionInOrder(tx, orderId, dependsOnId);

        assertEquals(CheckResults.STATUS.OK, commitResult.getCheckStatus());

    }



//    @Test
//    public void createAssociationMushroomTest() throws CheckException, DataConnectionException {
//        // given
//        WriteInterface write = connection.getWriteInterface();
//        Branch branch = write.createDisconnectedBranch();
//
//        DictionaryModelStore modelStore = DictionaryModelStoreFactory.getModelStore();
//        CzarnaMamba mambaModel = new CzarnaMamba(modelStore);
//
//
//
//        final Geometry line = EmptyGeometry.instance();
//        Feature<? extends Geometry> feature = createFeature(branch, line, mambaModel.FEATURES.Layer);
//
//        System.out.println("Feature Id: " + feature.getId() + " branch: " + branch.getBranchId());
//
//        setFeatureAttribute(branch, feature, mambaModel.FEATURES.LayerAttribute.branchId, "Taki oto branch1");
//        setFeatureAttribute(branch, feature, mambaModel.FEATURES.LayerAttribute.branchId, "Taki oto branch2");
//
//    }

    private void setFeatureAttribute(Branch branch, final Feature<? extends Geometry> feature, final DictionaryProperty type, final Object value)
        throws CheckException, DataConnectionException {
        WriteInterface write = connection.getWriteInterface();
        final Transaction tx1 = write.newTransaction(branch);
        MutableObjectManipulator objectManipulator = new MutableObjectManipulator();
        Attribute attribute = objectManipulator.createMutableAttribute(type, value);
        FeatureAttributesToSet featureAttributesToSet = FeatureAttributesToSetBuilder.getInstance()
            .withFeatureId(feature.getId())
            .withAttributesToSetForType(type, attribute)
            .withEditOptions(EditOptionsFactory.getDefaultEditOptions())
            .build();

        tx1.getBasicOperations().setFeatureAttributes(featureAttributesToSet, tx1.getNextGroupId());
        write.commitTransaction(tx1);
    }

    private Feature<? extends Geometry> createFeature(final Branch branch, final Geometry line, final DictionaryFeature type) throws CheckException, DataConnectionException {

        WriteInterface write = connection.getWriteInterface();

        final Transaction tx1 = write.newTransaction(branch);


        Delta delta = tx1.createGeometry(EmptyGeometry.instance(), type, EditOptionsFactory.withoutCascades(), null);
        System.out.println("Created " + delta.getModifications(ModificationType.ADDED).size());

        UUID createdFeatureUUID = getOnlyCreated(delta, type);


        write.commitTransaction(tx1);

        return connection.getReadInterface().getFeatureById(createdFeatureUUID,  ParametersBuilder.newBuilder().withBranch(branch).buildWIthoutSpatialFilter());
    }

    private UUID getOnlyCreated(final Delta delta, DictionaryFeature type) {
        Collection<FeatureModification> featuresModifications = delta.getFeaturesModifications(ModificationType.ADDED,
            FeatureTypeFilterFactory.createSelectedFeatureTypesFilter(type));
        FeatureModification onlyElement = Iterables.getOnlyElement(featuresModifications);
        return onlyElement.getObjectId();
    }

    @Test
    public void check() throws DataConnectionException {
        final Branch branch = new Branch(UUID.fromString(BRANCH));
        final Version currentVersion = connection.getJournalInterface().getCurrentVersion(branch);
        final MapView mapView = connection.mapView(branch, new Version(currentVersion.getJournalVersion() - 5000));
        final UUID usaUUID = getCountryUUID("USA", mapView);
        final UUID txUUID = getAdminAreaUUID(usaUUID, "TX", mapView);
        assertThat(txUUID).isNotNull();
    }

    @Test
    public void check_feature_read() throws DataConnectionException {

        Geometry geom = new Coordinate(0,0);
        final com.tomtom.cpu.coredb.client.filters.Parameters parameters =
            ParametersBuilder.newBuilder()
                .withBranch(new Branch(UUID.fromString("31703f97-9cee-43d9-bef0-ae1590558eb9")))
                .withVersion(new Version(496L))
                .withAssociations(ttom.TTOM_Core.ASSOCS.RoadElementOfRoad, ttom.TTOM_Core.ASSOCS.RoadHasBeginIntersection, ttom.TTOM_Core.ASSOCS.RoadHasEndIntersection)
                //.withSpatialFilter(new StrictSpatialFilter(geom))
                //.build();
                .buildWIthoutSpatialFilter();

        List<UUID> featureUUIDS =
            Stream.of("0000594b-4f00-0400-0000-000000313963","0000594b-4f00-0300-0000-0000001106fc","0000594b-4f00-0300-0000-000000110b3a","0000594b-4f00-4600-0000-000017e5734b","0000594b-4f00-0300-0000-00000006f16c","0000594b-4f00-0400-0000-000000313967","0000594b-4f00-4600-0000-00000bf41f3e","0000594b-4f00-4600-0000-00000bf2cc58","0000594b-4f00-4600-0000-00000bf44428","0000594b-4f00-0300-0000-000000114d26","0000594b-4f00-4600-0000-00000bf3e637","0000594b-4f00-4600-0000-00001808c406","0000594b-4f00-0300-0000-000000110b40","0000594b-4f00-4600-0000-00000bf3e635","0000594b-4f00-0300-0000-0000001106f8","00005858-5800-2800-0000-000077359822","0000594b-4f00-2800-ffff-ffffd036b026","0000594b-4f00-2800-ffff-ffffd044d906","0000594b-4f00-0300-0000-0000001116b3","0000594b-4f00-2800-ffff-ffffd04662e7","0000594b-4f00-4600-0000-0000009896e6","0000594b-4f00-0300-0000-000000114d27","0000594b-4f00-4600-0000-00000bf36dbe","00005858-5800-4600-0000-00007735951b")
                .map(UUID::fromString)
                .collect(Collectors.toList());

        List<Feature<? extends Geometry>> result =
            connection.getReadInterface().getFeaturesByIds(featureUUIDS, parameters);


        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());

        for (Feature f : result) {
            Collection<Association> associations = f.getAssociations();
            for (Association asoc : associations) {
                Feature<? extends Geometry> target = asoc.getTargetFeature();
                System.out.println("Association " + asoc.getId() + " source " + f.getId() + " taget " + asoc.getTargetFeature().getId()
                    + " taget assocs " + asoc.getTargetFeature().getAssociations().stream().map(a->a.getTargetFeature().getId()).collect(Collectors.toList()));

            }
        }

        featureUUIDS.removeAll(result.stream().map(f -> f.getId()).collect(Collectors.toList()));

        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());



        result.forEach(f-> f.getAssociations().stream().map(a->a.getTargetFeature()).forEach(tf -> System.out.println("Target feature: " + tf.getId())));

    }



    @Test
    public void check_feature_read_with_assoc() throws DataConnectionException {

        Geometry geom = new Coordinate(0,0);
        final com.tomtom.cpu.coredb.client.filters.Parameters parameters =
            ParametersBuilder.newBuilder()
                .withBranch(new Branch(UUID.fromString("31703f97-9cee-43d9-bef0-ae1590558eb9")))
                .withVersion(new Version(496L))
                .withAssociations(ttom.TTOM_Core.ASSOCS.RoadElementOfRoad, ttom.TTOM_Core.ASSOCS.RoadHasBeginIntersection, ttom.TTOM_Core.ASSOCS.RoadHasEndIntersection)
                //.withSpatialFilter(new StrictSpatialFilter(geom))
                //.build();
                .buildWIthoutSpatialFilter();

        List<UUID> featureUUIDS =
            Stream.of("b34d99d7-1e09-46d1-b0cb-cb305eae492d",
                    "94017147-cb77-4b6a-b79d-f59d97dd6854",
                    "9797e3b8-b06e-4a78-9a46-c0f1afd63c1d",
                    "0000594b-4f00-4600-0000-000017ea6d3b",
                    "0000594b-4f00-4600-0000-0000180e05f1")
                .map(UUID::fromString)
                .collect(Collectors.toList());

        List<Feature<? extends Geometry>> result =
            connection.getReadInterface().getFeaturesByIds(featureUUIDS, parameters);


        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());

        for (Feature f : result) {
            Collection<Association> associations = f.getAssociations();
            for (Association asoc : associations) {
                Feature<? extends Geometry> target = asoc.getTargetFeature();
                System.out.println("Association " + asoc.getId() + " source " + f.getId() + " taget " + asoc.getTargetFeature().getId()
                    + " taget assocs " + asoc.getTargetFeature().getAssociations().stream().map(a->a.getTargetFeature().getId()).collect(Collectors.toList()));

            }
        }

        featureUUIDS.removeAll(result.stream().map(f -> f.getId()).collect(Collectors.toList()));

        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());



        result.forEach(f-> f.getAssociations().stream().map(a->a.getTargetFeature()).forEach(tf -> System.out.println("Target feature: " + tf.getId())));

    }


    @Test
    public void check_feature_read_with_attributes() throws DataConnectionException {

        Geometry geom = new Coordinate(0,0);
        final com.tomtom.cpu.coredb.client.filters.Parameters parameters =
            ParametersBuilder.newBuilder()
                .withBranch(new Branch(UUID.fromString("31703f97-9cee-43d9-bef0-ae1590558eb9")))
                .withVersion(new Version(496L))
                //.withSpatialFilter(new StrictSpatialFilter(geom))
                //.build();
                .buildWIthoutSpatialFilter();

        List<UUID> featureUUIDS =
            Stream.of("0000594b-4f00-0400-0000-000000313963","0000594b-4f00-0300-0000-0000001106fc","0000594b-4f00-0300-0000-000000110b3a","0000594b-4f00-4600-0000-000017e5734b","0000594b-4f00-0300-0000-00000006f16c","0000594b-4f00-0400-0000-000000313967","0000594b-4f00-4600-0000-00000bf41f3e","0000594b-4f00-4600-0000-00000bf2cc58","0000594b-4f00-4600-0000-00000bf44428","0000594b-4f00-0300-0000-000000114d26","0000594b-4f00-4600-0000-00000bf3e637","0000594b-4f00-4600-0000-00001808c406","0000594b-4f00-0300-0000-000000110b40","0000594b-4f00-4600-0000-00000bf3e635","0000594b-4f00-0300-0000-0000001106f8","00005858-5800-2800-0000-000077359822","0000594b-4f00-2800-ffff-ffffd036b026","0000594b-4f00-2800-ffff-ffffd044d906","0000594b-4f00-0300-0000-0000001116b3","0000594b-4f00-2800-ffff-ffffd04662e7","0000594b-4f00-4600-0000-0000009896e6","0000594b-4f00-0300-0000-000000114d27","0000594b-4f00-4600-0000-00000bf36dbe","00005858-5800-4600-0000-00007735951b")
                .map(UUID::fromString)
                .collect(Collectors.toList());

        List<Feature<? extends Geometry>> result =
            connection.getReadInterface().getFeaturesByIds(featureUUIDS, parameters);


        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());

        for (Feature f : result) {
            Collection<Association> associations = f.getAssociations();
            for (Association asoc : associations) {
                Feature<? extends Geometry> target = asoc.getTargetFeature();
                System.out.println("Association " + asoc.getId() + " source " + f.getId() + " taget " + asoc.getTargetFeature().getId()
                    + " taget assocs " + asoc.getTargetFeature().getAssociations().stream().map(a->a.getTargetFeature().getId()).collect(Collectors.toList()));

            }
        }

        featureUUIDS.removeAll(result.stream().map(f -> f.getId()).collect(Collectors.toList()));

        System.out.println("Found " + result.size() + " features expected " + featureUUIDS.size());



        result.forEach(f-> f.getAssociations().stream().map(a->a.getTargetFeature()).forEach(tf -> System.out.println("Target feature: " + tf.getId())));

    }

    class SQ {
        int a;

        public SQ(int a) {

            this.a = a;
        }

        @Override public boolean equals(Object o) {

            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            SQ sq = (SQ)o;
            return a == sq.a;
        }

        @Override public int hashCode() {

            return Objects.hash(a);
        }
    }

    @Test
    public void testWeakReferences() {

        Map<Integer, SQ> map = Collections.synchronizedMap(new WeakHashMap<>());
        map.put(1, new SQ(10));

        map.computeIfPresent(1, (key, val) -> {map.remove(key); return null;});

        assertEquals(0, map.size());
    }


    @Test
    public void massiveCommitInOrderTest() throws Throwable {
        ExecutorService pool = Executors.newCachedThreadPool();

        String orderId = "ORDER_";
        String dependsOnOrderId = null;

        int rows = 12;
        int cols = 2;

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
            .filter(result -> result.getCheckStatus() != CheckResults.STATUS.OK)
            .findAny();

        anyError.ifPresent(result -> assertThat(result.getCheckStatus()).isEqualTo(CheckResults.STATUS.OK));
    }

    private ThrowingSupplier<CheckResults, Exception> createEditFuture(Branch branch, String orderId, int cols, int i, int n) {
        LOGGER.info("Create feature with orderId {} row {} col {} at thread {}" , i, n, Thread.currentThread().getName());
        int shift =  50000;
        int x0 = 5788 + i * shift;
        int y0 = 499 + shift * n;
        Coordinate[] coordinates =
            {new Coordinate(x0, y0), new Coordinate(x0 + 1, y0 + 1)};
        return createGeometryInTx(branch, orderId + (i * cols + n), n == 0 ? null : orderId + (i * cols), coordinates);
    }

    private ThrowingSupplier<CheckResults, Exception> createGeometryInTx(Branch branch, String orderId, String dependsOnOrderId, Coordinate[] coordinates) {
        Transaction tx = getWrite().newTransaction(branch);
        createAndGetCreatedFeatures(tx, Arrays.asList(coordinates));

        return () -> {
            LOGGER.info("Commit orderId {} depending on {} at thread {}" ,orderId, dependsOnOrderId, Thread.currentThread().getName());
            return getWrite().commitTransactionInOrder(tx, orderId, dependsOnOrderId);
        };
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
