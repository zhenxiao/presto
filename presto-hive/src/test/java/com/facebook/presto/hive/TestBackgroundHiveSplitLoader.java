/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.HiveColumnHandle.ColumnType;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.util.Progressable;
import org.junit.rules.TemporaryFolder;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static com.facebook.presto.hive.HiveColumnHandle.pathColumnHandle;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveUtil.getRegularColumnHandles;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String DEFAULT_TABLE_LOCATION = "hdfs://VOL1:9000/db_name/table_name";
    private static final String SAMPLE_PATH = DEFAULT_TABLE_LOCATION + "/000000_0";
    private static final String SAMPLE_PATH_FILTERED = DEFAULT_TABLE_LOCATION + "/000000_1";
    private static final String DEFAULT_TABLE_NAME = "test_table";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final TupleDomain<HiveColumnHandle> RETURNED_PATH_DOMAIN = withColumnDomains(
            ImmutableMap.of(
                    pathColumnHandle(),
                    Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()))));

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH),
            locatedFileStatus(FILTERED_PATH));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            new HiveColumnHandle("col1", HIVE_INT, INTEGER.getTypeSignature(), 0, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKET_COUNT, ImmutableList.of()));

    private static final StorageFormat DEFAULT_STORAGE_FORMAT = StorageFormat.create(
            "com.facebook.hive.orc.OrcSerde",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat");

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty(), DEFAULT_STORAGE_FORMAT, DEFAULT_TABLE_LOCATION, DEFAULT_TABLE_NAME);
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY, DEFAULT_STORAGE_FORMAT, DEFAULT_TABLE_LOCATION, DEFAULT_TABLE_NAME);
    private static final String RAW_TRIPS_TABLE_NAME = "raw_trips";
    private static final String HOODIE_INPUT_FORMAT_CANONICAL_NAME = HoodieInputFormat.class.getCanonicalName();
    private static final StorageFormat HOODIE_STORAGE_FORMAT = StorageFormat.create(
            "any String.",
            HOODIE_INPUT_FORMAT_CANONICAL_NAME,
            HOODIE_INPUT_FORMAT_CANONICAL_NAME);
    private static final TemporaryFolder HOODIE_TABLE_BASE_PATH = new TemporaryFolder();
    private static Table hoodieTable;

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, TupleDomain.none());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.of(new HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKET_COUNT)),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE),
                                BUCKET_COUNT)),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatusWithNoBlocks(RETURNED_PATH)),
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, TupleDomain.none());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, TupleDomain.all());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, () -> hiveSplitSource.isFinished());
    }

    @Test
    public void testCachedDirectoryLister()
            throws Exception
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig()
                .setFileStatusCacheMaxSize(1000)
                .setFileStatusCacheExpireAfterWrite(new Duration(5, TimeUnit.MINUTES))
                .setFileStatusCacheTables("test_dbname.test_table")
                .setMaxSplitSize(new DataSize(1.0, GIGABYTE));

        CachedDirectoryLister cachedDirectoryLister = new CachedDirectoryLister(hiveClientConfig);
        assertEquals(cachedDirectoryLister.getRequestCount(), 0);

        int totalCount = 1000;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(EXECUTOR.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachedDirectoryLister);
            HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, TupleDomain.none());
            backgroundHiveSplitLoader.start(hiveSplitSource);
            try {
                return drainSplits(hiveSplitSource);
            }
            finally {
                firstVisit.countDown();
            }
        }));

        for (int i = 0; i < totalCount - 1; i++) {
            futures.add(EXECUTOR.submit(() -> {
                firstVisit.await();
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachedDirectoryLister);
                HiveSplitSource hiveSplitSource = hiveSplitSource(SIMPLE_TABLE, backgroundHiveSplitLoader, TupleDomain.none());
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertEquals(future.get().size(), 2);
        }
        assertEquals(cachedDirectoryLister.getRequestCount(), totalCount);
        assertEquals(cachedDirectoryLister.getHitCount(), totalCount - 1);
    }

    @Test
    public void testIfHoodieInputFormat()
    {
        BackgroundHiveSplitLoader dummySplitLoader = backgroundHiveSplitLoader(ImmutableList.of(locatedFileStatus(RETURNED_PATH)), TupleDomain.none());
        assertTrue(dummySplitLoader.isHoodieInputFormat(HOODIE_INPUT_FORMAT_CANONICAL_NAME));
    }

    @Test
    public void testLoadSplitsForHoodieTable() throws Exception
    {
        //prepare Hoodie DataSet
        int numFiles = 19;
        String commitnumber = "123";
        Pair<File, List<LocatedFileStatus>> partitionPathToFiles = prepareHoodieDataset(HOODIE_TABLE_BASE_PATH, numFiles, commitnumber);

        // create the commit file in Hoodie Metadata
        new File(HOODIE_TABLE_BASE_PATH.getRoot().toString() + "/.hoodie/", commitnumber + ".commit").createNewFile();

        BackgroundHiveSplitLoader splitLoader = backgroundHiveSplitLoader(partitionPathToFiles.getRight(),
                TupleDomain.none(),
                Optional.empty(),
                hoodieTable,
                Optional.empty(),
                Optional.of(ImmutableSet.of(HOODIE_INPUT_FORMAT_CANONICAL_NAME)));
        HiveSplitSource hiveSplitSource = hiveSplitSource(hoodieTable, splitLoader, TupleDomain.none());
        splitLoader.start(hiveSplitSource);
        List<HiveSplit> hiveSplits = drainSplits(hiveSplitSource);
        assertEquals(hiveSplits.size(), numFiles);

        // Check split locations match the datafiles created
        Set<String> hiveSplitpaths = new HashSet<>();
        hiveSplits.stream().map(s -> s.getPath()).forEach(hiveSplitpaths::add);
        for (LocatedFileStatus fileStatus : partitionPathToFiles.getRight()) {
            assertTrue(hiveSplitpaths.contains(fileStatus.getPath().toString()), "Encountered unknown split!");
        }
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            source.getNextBatch(NOT_PARTITIONED, 100).get()
                    .getSplits().stream()
                    .map(HiveSplit.class::cast)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> tupleDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                tupleDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty(),
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<Set<String>> respectSplitsInputFormats)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                compactEffectivePredicate,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                new CachedDirectoryLister(new HiveClientConfig()),
                EXECUTOR,
                2,
                false,
                Optional.empty(),
                Optional.empty(),
                respectSplitsInputFormats.isPresent() ? respectSplitsInputFormats.get() : ImmutableSet.of(),
                Optional.empty(),
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(List<LocatedFileStatus> files, DirectoryLister directoryLister)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                hivePartitionMetadatas,
                TupleDomain.none(),
                Optional.empty(),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                EXECUTOR,
                2,
                false,
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachedDirectoryLister(new HiveClientConfig()),
                directExecutor(),
                2,
                false,
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                false);
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<HivePartitionMetadata>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
            }
        };
    }

    private static HiveSplitSource hiveSplitSource(
            Table table,
            BackgroundHiveSplitLoader backgroundHiveSplitLoader,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                table.getDatabaseName(),
                table.getTableName(),
                compactEffectivePredicate,
                1,
                1,
                new DataSize(32, MEGABYTE),
                backgroundHiveSplitLoader,
                EXECUTOR,
                new CounterStat());
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            StorageFormat storageFormat,
            String location,
            String tableName)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(storageFormat)
                .setLocation(location)
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName(tableName)
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of())
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static Pair<File, List<LocatedFileStatus>> prepareHoodieDataset(TemporaryFolder basePath, int numberOfFiles,
                                                                            String commitNumber) throws IOException
    {
        HOODIE_TABLE_BASE_PATH.delete();
        HOODIE_TABLE_BASE_PATH.create();
        hoodieTable = table(PARTITION_COLUMNS, Optional.empty(), HOODIE_STORAGE_FORMAT, HOODIE_TABLE_BASE_PATH.getRoot().toString(), RAW_TRIPS_TABLE_NAME);
        HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, RAW_TRIPS_TABLE_NAME);
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
        properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, HoodieAvroPayload.class.getName());
        HoodieTableMetaClient.initializePathAsHoodieDataset(new Configuration(), basePath.getRoot().toString(), properties);
        File partitionPath = basePath.newFolder("2016", "05", "01");
        List<LocatedFileStatus> filesInPartition = new ArrayList<>();
        for (int i = 0; i < numberOfFiles; i++) {
            File dataFile = new File(partitionPath,
                    FSUtils.makeDataFileName(commitNumber, 1, "fileid" + i));
            dataFile.createNewFile();
            filesInPartition.add(locatedFileStatus(new Path(dataFile.getAbsolutePath())));
        }
        Pair<File, List<LocatedFileStatus>> result = new ImmutablePair<>(partitionPath, filesInPartition);
        return result;
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation()});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationUpdater(new HiveClientConfig())),
                    new HiveClientConfig(),
                    new NoHdfsAuthentication());
            this.files = files;
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = files;
        }

        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
                throws FileNotFoundException, IOException
        {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}
