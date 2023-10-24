package org.apache.paimon.flink;


import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.paimon.catalog.AbstractCatalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;

public class FlinkExternalCatalog extends AbstractCatalog {

    private final FlinkCatalog paimon;
    private final String warehousePath;

    private final static String EXTERNAL_TABLE_STORE_PATH = ".FLINK_EXTERNAL_TABLE";
    private final static String EXTERNAL_FUNCTION_STORE_PATH = ".FLINK_EXTERNAL_FUNCTION";
    private final FileIO fileIO;

    public FlinkExternalCatalog(FlinkCatalog paimon, FileIO fileIO, String warehousePath) {
        super(paimon.getName(), paimon.getDefaultDatabase());
        this.paimon = paimon;
        this.fileIO = fileIO;
        this.warehousePath = warehousePath;
        try {
            fileIO.mkdirs(new Path(externalTableDir()));
        } catch (IOException ignore) {

        }
        try {
            fileIO.mkdirs(new Path(externalFunctionDir()));
        } catch (IOException ignore) {

        }
    }

    private String systemDb() {
        return this.warehousePath + Path.SEPARATOR + SYSTEM_DATABASE_NAME + DB_SUFFIX + Path.SEPARATOR;
    }

    private String externalTableDir() {
        return systemDb() + EXTERNAL_TABLE_STORE_PATH + Path.SEPARATOR;
    }

    private String externalFunctionDir() {
        return systemDb() + EXTERNAL_FUNCTION_STORE_PATH + Path.SEPARATOR;
    }

    private String externalTableSchemaDir(ObjectPath table) {
        return externalTableDir() + Path.SEPARATOR + table.getDatabaseName() + Path.SEPARATOR + table.getObjectName() + Path.SEPARATOR;
    }

    @Override
    public void open() throws CatalogException {
        paimon.open();
    }

    @Override
    public void close() throws CatalogException {
        paimon.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return paimon.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return paimon.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        paimon.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        paimon.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        paimon.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }

    private boolean isPaimonTable(CatalogBaseTable catalogBaseTable) {
        Map<String, String> options = catalogBaseTable.getOptions();
        String connector = options.get(CONNECTOR.key());
        return StringUtils.isNullOrWhitespaceOnly(connector)
                || FlinkCatalogFactory.IDENTIFIER.equals(connector);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Path databasePath = new Path(externalTableDir() + Path.SEPARATOR + databaseName);
        try {
            FileStatus[] fileStatuses = fileIO.listStatus(databasePath);
            List<String> tableList = Arrays.stream(fileStatuses).map(FileStatus::getPath).map(Path::getName)
                    .collect(Collectors.toList());
            tableList.addAll(paimon.listTables(databaseName));
            return tableList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.listViews(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        try {
            return paimon.getTable(tablePath);
        } catch (TableNotExistException | CatalogException e) {
            Path tableSchemaPath = new Path(externalTableSchemaDir(tablePath) + "schema");
            try {
                String schemaStr = fileIO.readFileUtf8(tableSchemaPath);
                Map<String, Object> propertiesMap = new ObjectMapper().readValue(schemaStr, new TypeReference<HashMap<String, Object>>() {
                });
                Map<String,String> properties = new HashMap<>();
                propertiesMap.forEach((key,value)->{
                    properties.put(key,String.valueOf(value));
                });
                return CatalogTable.fromProperties(properties);
            } catch (IOException ex) {
                throw new CatalogException("can not read external table", ex);
            }
        }
    }

//    private CatalogTable createCatalogTable(ExternalTableSchema externalTableSchema) {
//
//
//        return CatalogTable.of(schema, externalTableSchema.comment,
//                externalTableSchema.partitionKeys, externalTableSchema.options);
//    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (isPaimonTable(table)) {
            paimon.createTable(tablePath, table, ignoreIfExists);
        } else {
            if (table instanceof CatalogTable) {
                CatalogTable catalogTable = (CatalogTable) table;
//                ExternalTableSchema externalTableSchema = new ExternalTableSchema();
//                Schema unresolvedSchema = catalogTable.getUnresolvedSchema();
//                externalTableSchema.setComment(catalogTable.getComment());
//                externalTableSchema.setOptions(catalogTable.getOptions());
//                externalTableSchema.setPartitionKeys(catalogTable.getPartitionKeys());
                String tableMetaJson = JsonSerdeUtil.toJson(catalogTable.toProperties());
                Path tableSchemaPath = new Path(externalTableSchemaDir(tablePath) + "schema");
                try {
                    fileIO.writeFileUtf8(tableSchemaPath, tableMetaJson);
                } catch (IOException e) {
                    throw new CatalogException("can not create external table", e);
                }
            } else {
                throw new CatalogException("can not create external table");
            }
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(getTable(tablePath))) {
            paimon.alterTable(tablePath, newTable, ignoreIfNotExists);
        } else {

        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return paimon.listPartitions(tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, CatalogException {
        return paimon.listPartitions(tablePath, partitionSpec);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return paimon.listPartitionsByFilter(tablePath, filters);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartition(tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return paimon.partitionExists(tablePath, partitionSpec);
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, PartitionAlreadyExistsException,
            CatalogException {
        paimon.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return paimon.getTableStatistics(tablePath);
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return paimon.getTableColumnStatistics(tablePath);
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartitionStatistics(tablePath, partitionSpec);
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartitionColumnStatistics(tablePath, partitionSpec);
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        paimon.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        paimon.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartitionStatistics(tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartitionColumnStatistics(tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
    }


}
