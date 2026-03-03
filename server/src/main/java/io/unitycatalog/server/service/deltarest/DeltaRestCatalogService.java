package io.unitycatalog.server.service.deltarest;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.model.DeltaGetCommitsResponse;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.model.deltarest.CatalogConfig;
import io.unitycatalog.server.model.deltarest.CatalogConfigOverrides;
import io.unitycatalog.server.model.deltarest.CreateNamespaceRequest;
import io.unitycatalog.server.model.deltarest.CreateStagingTableRequest;
import io.unitycatalog.server.model.deltarest.CreateTableRequest;
import io.unitycatalog.server.model.deltarest.CredentialsResponse;
import io.unitycatalog.server.model.deltarest.DeltaColumn;
import io.unitycatalog.server.model.deltarest.DeltaProtocol;
import io.unitycatalog.server.model.deltarest.ListNamespacesResponse;
import io.unitycatalog.server.model.deltarest.TableIdentifierWithDataSourceFormat;
import io.unitycatalog.server.model.deltarest.LoadTableResponse;
import io.unitycatalog.server.model.deltarest.NamespaceResponse;
import io.unitycatalog.server.model.deltarest.RenameTableRequest;
import io.unitycatalog.server.model.deltarest.ReportMetricsRequest;
import io.unitycatalog.server.model.deltarest.StagingTableResponse;
import io.unitycatalog.server.model.deltarest.StorageCredential;
import io.unitycatalog.server.model.deltarest.StructField;
import io.unitycatalog.server.model.deltarest.TableMetadata;
import io.unitycatalog.server.model.deltarest.UpdateNamespacePropertiesRequest;
import io.unitycatalog.server.model.deltarest.UpdateNamespacePropertiesResponse;
import io.unitycatalog.server.model.deltarest.UpdateTableRequest;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.DeltaCommitRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.StagingTableRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.AuthorizedService;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

/**
 * Delta REST Catalog Service - IRC-style API for Delta tables.
 *
 * <p>This service follows the Apache Iceberg REST Catalog (IRC) API style but is Delta-centric.
 * Unlike the actual IRC APIs implemented for UniForm and Managed Iceberg tables, this API
 * does not provide a translation layer to make Delta tables work like Iceberg tables.
 */
@ExceptionHandler(DeltaRestExceptionHandler.class)
public class DeltaRestCatalogService extends AuthorizedService {

  private static final String PREFIX_TEMPLATE = "catalogs/%s";

  private static final List<String> ENDPOINTS = List.of(
      // Table operations
      "POST /v1/{prefix}/namespaces/{namespace}/staging-tables",
      "POST /v1/{prefix}/namespaces/{namespace}/tables",
      "GET /v1/{prefix}/namespaces/{namespace}/tables",
      "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
      "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
      "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
      "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
      "POST /v1/{prefix}/tables/rename",
      "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
      "GET /v1/{prefix}/namespaces/{namespace}/staging-tables/{table_id}/credentials",
      "POST /v1/temporary-path-credentials",
      // Schema operations
      "GET /v1/{prefix}/namespaces",
      "GET /v1/{prefix}/namespaces/{namespace}",
      "POST /v1/{prefix}/namespaces",
      "POST /v1/{prefix}/namespaces/{namespace}/properties",
      "DELETE /v1/{prefix}/namespaces/{namespace}",
      "HEAD /v1/{prefix}/namespaces/{namespace}",
      // Metrics
      "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"
  );

  private final TableRepository tableRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final StagingTableRepository stagingTableRepository;
  private final DeltaCommitRepository deltaCommitRepository;
  private final StorageCredentialVendor storageCredentialVendor;

  @SneakyThrows
  public DeltaRestCatalogService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      StorageCredentialVendor storageCredentialVendor) {
    super(authorizer, repositories);
    this.tableRepository = repositories.getTableRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.stagingTableRepository = repositories.getStagingTableRepository();
    this.deltaCommitRepository = repositories.getDeltaCommitRepository();
    this.storageCredentialVendor = storageCredentialVendor;
  }

  // ==================== Configuration API ====================

  @Get("/v1/config")
  @ProducesJson
  public HttpResponse getConfig(@Param("catalog") String catalog) {
    if (catalog == null || catalog.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT,
          "Must supply a proper catalog in catalog parameter.");
    }

    // Verify catalog exists
    catalogRepository.getCatalog(catalog);

    CatalogConfigOverrides overrides = new CatalogConfigOverrides();
    overrides.setPrefix(String.format(PREFIX_TEMPLATE, catalog));

    CatalogConfig response = new CatalogConfig();
    response.setOverrides(overrides);
    response.setEndpoints(ENDPOINTS);
    response.setManagedTablesRequiredFeatures(List.of(
        "appendOnly", "catalogManaged", "deletionVectors", "inCommitTimestamp",
        "invariants", "v2Checkpoint", "vacuumProtocolCheck"
    ));
    response.setManagedTablesSuggestedFeatures(List.of(
        "rowTracking", "domainMetadata"
    ));

    return HttpResponse.ofJson(response);
  }

  // ==================== Staging Table APIs ====================

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/staging-tables")
  @ProducesJson
  @AuthorizeExpression("""
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
       && #authorize(#principal, #schema, OWNER)) ||
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
       && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createStagingTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      CreateStagingTableRequest request) {

    CreateStagingTable createStagingTable = new CreateStagingTable()
        .catalogName(catalog)
        .schemaName(namespace)
        .name(request.getName());

    StagingTableInfo stagingTableInfo =
        stagingTableRepository.createStagingTable(createStagingTable);

    SchemaInfo schemaInfo = schemaRepository.getSchema(catalog + "." + namespace);
    initializeHierarchicalAuthorization(stagingTableInfo.getId(), schemaInfo.getSchemaId());

    // Build response in DRC format
    StagingTableResponse response = new StagingTableResponse();
    response.setTableId(UUID.fromString(stagingTableInfo.getId()));
    response.setTableType(StagingTableResponse.TableTypeEnum.MANAGED);
    response.setLocation(stagingTableInfo.getStagingLocation());

    // Vend initial credentials
    NormalizedURL storageLocation = NormalizedURL.from(stagingTableInfo.getStagingLocation());
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(storageLocation, Set.of(SELECT, UPDATE));
    response.setStorageCredentials(
        buildStorageCredentials(stagingTableInfo.getStagingLocation(), credentials));

    return HttpResponse.ofJson(response);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/staging-tables/{table_id}/credentials")
  @ProducesJson
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #table, OWNER, MODIFY)
      """)
  public HttpResponse getStagingTableCredentials(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table_id") @AuthorizeResourceKey(TABLE) String tableId) {

    NormalizedURL storageLocation =
        tableRepository.getStorageLocationForStagingTable(UUID.fromString(tableId));
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(storageLocation, Set.of(SELECT, UPDATE));

    CredentialsResponse response =
        new CredentialsResponse()
            .storageCredentials(buildStorageCredentials(storageLocation.toString(), credentials));

    return HttpResponse.ofJson(response);
  }

  // ==================== Table CRUD APIs ====================

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  @AuthorizeExpression("""
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#authorize(#principal, #schema, OWNER) ||
        #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      CreateTableRequest request) {

    String tableName = request.getName();
    String location = request.getLocation();

    if (tableName == null || tableName.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required");
    }
    if (location == null || location.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Location is required");
    }

    // Get table type from request
    CreateTableRequest.TableTypeEnum reqTableType = request.getTableType();
    if (reqTableType == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table type is required");
    }
    TableType tableType = TableType.fromValue(reqTableType.getValue());

    io.unitycatalog.server.model.deltarest.DataSourceFormat reqFormat =
        request.getDataSourceFormat();
    String formatValue = reqFormat != null ? reqFormat.getValue() : "DELTA";

    CreateTable createTable = new CreateTable()
        .catalogName(catalog)
        .schemaName(namespace)
        .name(tableName)
        .tableType(tableType)
        .dataSourceFormat(DataSourceFormat.fromValue(formatValue))
        .storageLocation(location);

    // Handle comment if provided
    String comment = request.getComment();
    if (comment != null) {
      createTable.comment(comment);
    }

    // Handle columns/schema if provided
    List<DeltaColumn> schemaColumns = request.getSchema();
    if (schemaColumns != null && !schemaColumns.isEmpty()) {
      createTable.columns(TableRepository.convertDeltaColumnsToColumnInfoList(schemaColumns));
    }

    // Handle properties if provided
    Map<String, String> properties = request.getProperties();
    if (properties != null) {
      createTable.properties(properties);
    }

    TableInfo tableInfo = tableRepository.createTable(createTable);

    SchemaInfo schemaInfo = schemaRepository.getSchema(catalog + "." + namespace);
    initializeHierarchicalAuthorization(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(buildLoadTableResponse(tableInfo, false));
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  @AuthorizeExpression("#defer")
  public HttpResponse listTables(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("pageToken") Optional<String> pageToken) {

    // Get UC tables response
    ListTablesResponse ucResponse = tableRepository.listTables(
        catalog, namespace, Optional.of(1000), pageToken, false, false);

    filterTables("""
        #authorize(#principal, #metastore, OWNER) ||
        #authorize(#principal, #catalog, OWNER) ||
        (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
        (#authorize(#principal, #schema, USE_SCHEMA) &&
            #authorize(#principal, #catalog, USE_CATALOG) &&
            #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
        """, ucResponse.getTables());

    // Convert to DRC format
    List<TableIdentifierWithDataSourceFormat> identifiers = new ArrayList<>();
    if (ucResponse.getTables() != null) {
      for (TableInfo table : ucResponse.getTables()) {
        TableIdentifierWithDataSourceFormat identifier = new TableIdentifierWithDataSourceFormat();
        identifier.setNamespace(List.of(table.getSchemaName()));
        identifier.setName(table.getName());

        // Convert UC DataSourceFormat to DRC DataSourceFormat
        io.unitycatalog.server.model.deltarest.DataSourceFormat drcFormat =
            table.getDataSourceFormat() != null
                ? io.unitycatalog.server.model.deltarest.DataSourceFormat.fromValue(
                    table.getDataSourceFormat().getValue())
                : io.unitycatalog.server.model.deltarest.DataSourceFormat.DELTA;
        identifier.setDataSourceFormat(drcFormat);

        identifiers.add(identifier);
      }
    }

    // Build DRC response
    io.unitycatalog.server.model.deltarest.ListTablesResponse drcResponse =
        new io.unitycatalog.server.model.deltarest.ListTablesResponse();
    drcResponse.setIdentifiers(identifiers);
    drcResponse.setNextPageToken(ucResponse.getNextPageToken());

    return HttpResponse.ofJson(drcResponse);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse loadTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      @Param("with_credentials") Optional<Boolean> withCredentials) {

    String fullName = catalog + "." + namespace + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);

    return HttpResponse.ofJson(buildLoadTableResponse(tableInfo, withCredentials.orElse(false)));
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #table, OWNER, MODIFY)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String tableName,
      UpdateTableRequest request) {

    String fullName = catalog + "." + namespace + "." + tableName;

    // Execute all operations in a single transaction via repository
    TableInfo tableInfo = tableRepository.updateTableWithDeltaRestRequest(fullName, request);

    return HttpResponse.ofJson(buildLoadTableResponse(tableInfo, false));
  }

  @Delete("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #table, OWNER))
      """)
  public HttpResponse deleteTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {

    String fullName = catalog + "." + namespace + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);
    tableRepository.deleteTable(fullName);

    SchemaInfo schemaInfo = schemaRepository.getSchema(catalog + "." + namespace);
    removeHierarchicalAuthorizations(tableInfo.getTableId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse tableExists(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {

    String fullName = catalog + "." + namespace + "." + table;
    try {
      tableRepository.getTable(fullName);
      return HttpResponse.of(HttpStatus.NO_CONTENT);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        return HttpResponse.of(HttpStatus.NOT_FOUND);
      }
      throw e;
    }
  }

  @Post("/v1/catalogs/{catalog}/tables/rename")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG))
      """)
  public HttpResponse renameTable(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      RenameTableRequest request) {

    if (request.getSource() == null || request.getDestination() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Source and destination are required");
    }

    List<String> sourceNamespace = request.getSource().getNamespace();
    String sourceName = request.getSource().getName();
    List<String> destNamespace = request.getDestination().getNamespace();
    String destName = request.getDestination().getName();

    // For now, only support rename within same namespace
    String sourceFullName = catalog + "." + sourceNamespace.get(0) + "." + sourceName;
    String destFullName = catalog + "." + destNamespace.get(0) + "." + destName;

    // TODO: Implement table rename in TableRepository
    throw new BaseException(ErrorCode.UNIMPLEMENTED, "Table rename not yet implemented");
  }

  // ==================== Table Credentials API ====================

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/credentials")
  @ProducesJson
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY)
      """)
  public HttpResponse getTableCredentials(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String table) {

    String fullName = catalog + "." + namespace + "." + table;
    TableInfo tableInfo = tableRepository.getTable(fullName);

    NormalizedURL storageLocation = NormalizedURL.from(tableInfo.getStorageLocation());
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(storageLocation, Set.of(SELECT, UPDATE));

    CredentialsResponse response =
        new CredentialsResponse()
            .storageCredentials(
                buildStorageCredentials(tableInfo.getStorageLocation(), credentials));

    return HttpResponse.ofJson(response);
  }

  @Get("/v1/temporary-path-credentials")
  @ProducesJson
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getTemporaryPathCredentials(
      @Param("location") String location,
      @Param("operation") String operation) {

    if (location == null || location.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Location is required");
    }

    Set<CredentialContext.Privilege> privileges = Set.of(SELECT, UPDATE);
    if ("PATH_CREATE_TABLE".equals(operation)) {
      privileges = Set.of(SELECT, UPDATE);
    }

    NormalizedURL storageLocation = NormalizedURL.from(location);
    TemporaryCredentials credentials =
        storageCredentialVendor.vendCredential(storageLocation, privileges);

    CredentialsResponse response =
        new CredentialsResponse()
            .storageCredentials(buildStorageCredentials(location, credentials));

    return HttpResponse.ofJson(response);
  }

  // ==================== Namespace (Schema) APIs ====================

  @Get("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("#defer")
  public HttpResponse listNamespaces(
      @Param("catalog") String catalog,
      @Param("pageToken") Optional<String> pageToken) {

    ListSchemasResponse response = schemaRepository.listSchemas(
        catalog, Optional.of(1000), pageToken);

    filterSchemas("""
        #authorize(#principal, #metastore, OWNER) ||
        #authorize(#principal, #catalog, OWNER) ||
        (#authorize(#principal, #schema, USE_SCHEMA) &&
            #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG))
        """, response.getSchemas());

    List<List<String>> namespaces = new ArrayList<>();
    if (response.getSchemas() != null) {
      for (SchemaInfo schema : response.getSchemas()) {
        namespaces.add(List.of(schema.getName()));
      }
    }

    ListNamespacesResponse result = new ListNamespacesResponse();
    result.setNamespaces(namespaces);
    result.setNextPageToken(response.getNextPageToken());

    return HttpResponse.ofJson(result);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @ProducesJson
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse loadNamespace(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace) {

    String fullName = catalog + "." + namespace;
    SchemaInfo schemaInfo = schemaRepository.getSchema(fullName);

    NamespaceResponse response = new NamespaceResponse();
    response.setNamespace(List.of(schemaInfo.getName()));

    Map<String, String> properties = new HashMap<>();
    if (schemaInfo.getOwner() != null) {
      properties.put("owner", schemaInfo.getOwner());
    }
    if (schemaInfo.getCreatedAt() != null) {
      properties.put("created_at", schemaInfo.getCreatedAt().toString());
    }
    if (schemaInfo.getComment() != null) {
      properties.put("description", schemaInfo.getComment());
    }
    if (schemaInfo.getSchemaId() != null) {
      properties.put("io.unitycatalog.schemaId", schemaInfo.getSchemaId());
    }
    if (schemaInfo.getProperties() != null) {
      properties.putAll(schemaInfo.getProperties());
    }
    response.setProperties(properties);

    return HttpResponse.ofJson(response);
  }

  @Post("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, CREATE_SCHEMA)
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse createNamespace(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      CreateNamespaceRequest request) {

    List<String> namespace = request.getNamespace();
    Map<String, String> properties = request.getProperties();

    if (namespace == null || namespace.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Namespace is required");
    }

    String schemaName = namespace.get(0);
    CreateSchema createSchema = new CreateSchema()
        .catalogName(catalog)
        .name(schemaName);

    if (properties != null) {
      if (properties.containsKey("description")) {
        createSchema.comment(properties.get("description"));
      }
      createSchema.properties(properties);
    }

    SchemaInfo schemaInfo = schemaRepository.createSchema(createSchema);

    CatalogInfo catalogInfo = catalogRepository.getCatalog(catalog);
    initializeHierarchicalAuthorization(schemaInfo.getSchemaId(), catalogInfo.getId());

    NamespaceResponse response = new NamespaceResponse();
    response.setNamespace(List.of(schemaInfo.getName()));

    Map<String, String> responseProperties = new HashMap<>();
    if (schemaInfo.getOwner() != null) {
      responseProperties.put("owner", schemaInfo.getOwner());
    }
    if (schemaInfo.getSchemaId() != null) {
      responseProperties.put("io.unitycatalog.schemaId", schemaInfo.getSchemaId());
    }
    if (properties != null) {
      responseProperties.putAll(properties);
    }
    response.setProperties(responseProperties);

    return HttpResponse.ofJson(response);
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/properties")
  @ProducesJson
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #schema, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, USE_SCHEMA) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateNamespaceProperties(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      UpdateNamespacePropertiesRequest request) {

    String fullName = catalog + "." + namespace;

    Map<String, String> updates = request.getUpdates();
    List<String> removals = request.getRemovals();

    UpdateSchema updateSchema = new UpdateSchema();
    if (updates != null) {
      if (updates.containsKey("name")) {
        updateSchema.newName(updates.get("name"));
      }
      if (updates.containsKey("comment") || updates.containsKey("description")) {
        updateSchema.comment(updates.getOrDefault("comment", updates.get("description")));
      }
    }

    schemaRepository.updateSchema(fullName, updateSchema);

    List<String> updated = updates != null ? new ArrayList<>(updates.keySet()) : List.of();
    List<String> removed = new ArrayList<>();
    List<String> missing = new ArrayList<>();
    if (removals != null) {
      // Properties that were requested to be removed but not found
      missing.addAll(removals);
    }

    UpdateNamespacePropertiesResponse response = new UpdateNamespacePropertiesResponse();
    response.setUpdated(updated);
    response.setRemoved(removed);
    response.setMissing(missing);

    return HttpResponse.ofJson(response);
  }

  @Delete("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteNamespace(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace) {

    String fullName = catalog + "." + namespace;
    SchemaInfo schemaInfo = schemaRepository.getSchema(fullName);
    schemaRepository.deleteSchema(fullName, false);

    CatalogInfo catalogInfo = catalogRepository.getCatalog(catalog);

    // First remove any child table links
    authorizer.removeHierarchyChildren(UUID.fromString(schemaInfo.getSchemaId()));
    // Then remove schema from catalog and clear authorizations
    removeHierarchicalAuthorizations(schemaInfo.getSchemaId(), catalogInfo.getId());

    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
          #authorizeAny(#principal, #catalog, USE_CATALOG))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse namespaceExists(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace) {

    String fullName = catalog + "." + namespace;
    try {
      schemaRepository.getSchema(fullName);
      return HttpResponse.of(HttpStatus.NO_CONTENT);
    } catch (BaseException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        return HttpResponse.of(HttpStatus.NOT_FOUND);
      }
      throw e;
    }
  }

  // ==================== Metrics API ====================

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY)
      """)
  public HttpResponse reportMetrics(
      @Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog,
      @Param("namespace") @AuthorizeResourceKey(SCHEMA) String namespace,
      @Param("table") @AuthorizeResourceKey(TABLE) String table,
      ReportMetricsRequest metrics) {
    // Accept and log metrics but don't process them for now
    return HttpResponse.of(HttpStatus.NO_CONTENT);
  }

  // ==================== Helper Methods ====================

  private LoadTableResponse buildLoadTableResponse(TableInfo tableInfo, boolean withCredentials) {
    LoadTableResponse response = new LoadTableResponse();

    // Build metadata
    TableMetadata metadata = new TableMetadata();
    metadata.setEtag(tableRepository.generateEtag(tableInfo));

    // Data source format
    io.unitycatalog.server.model.deltarest.DataSourceFormat drcFormat =
        io.unitycatalog.server.model.deltarest.DataSourceFormat.DELTA;
    if (tableInfo.getDataSourceFormat() != null) {
      drcFormat =
          io.unitycatalog.server.model.deltarest.DataSourceFormat.fromValue(
              tableInfo.getDataSourceFormat().getValue());
    }
    metadata.setDataSourceFormat(drcFormat);

    // Table type
    if (tableInfo.getTableType() != null) {
      metadata.setTableType(
          TableMetadata.TableTypeEnum.fromValue(tableInfo.getTableType().getValue()));
    } else {
      metadata.setTableType(TableMetadata.TableTypeEnum.MANAGED);
    }

    metadata.setTableUuid(UUID.fromString(tableInfo.getTableId()));
    metadata.setLocation(tableInfo.getStorageLocation());
    metadata.setOwner(tableInfo.getOwner());
    metadata.setComment(tableInfo.getComment());
    metadata.setCreateTime(tableInfo.getCreatedAt());
    metadata.setCreatedBy(tableInfo.getCreatedBy());
    metadata.setUpdateTime(tableInfo.getUpdatedAt());
    metadata.setUpdatedBy(tableInfo.getUpdatedBy());
    metadata.setSecurableType("TABLE");

    // Convert columns to DRC schema format
    if (tableInfo.getColumns() != null && !tableInfo.getColumns().isEmpty()) {
      List<DeltaColumn> schema =
          tableInfo.getColumns().stream()
              .map(this::convertColumnToDeltaColumn)
              .collect(Collectors.toList());
      metadata.setSchema(schema);
    } else {
      metadata.setSchema(List.of());
    }

    // Add protocol
    DeltaProtocol protocol = new DeltaProtocol();
    protocol.setMinReaderVersion(1);
    protocol.setMinWriterVersion(2);
    protocol.setReaderFeatures(List.of());
    protocol.setWriterFeatures(List.of("appendOnly", "inCommitTimestamp"));
    metadata.setProtocol(protocol);

    // Add properties
    metadata.setProperties(
        tableInfo.getProperties() != null ? tableInfo.getProperties() : Map.of());

    response.setMetadata(metadata);

    // Get commits if this is a managed Delta table
    if (tableInfo.getTableType() == TableType.MANAGED
        && tableInfo.getDataSourceFormat() == DataSourceFormat.DELTA) {
      try {
        DeltaGetCommits getCommitsRequest =
            new DeltaGetCommits()
                .tableId(tableInfo.getTableId())
                .tableUri(tableInfo.getStorageLocation())
                .startVersion(0L);
        DeltaGetCommitsResponse commitsResponse =
            deltaCommitRepository.getCommits(getCommitsRequest);
        if (commitsResponse.getCommits() != null) {
          List<io.unitycatalog.server.model.deltarest.DeltaCommit> commits =
              commitsResponse.getCommits().stream()
                  .map(this::convertCommitInfoToDeltaCommit)
                  .collect(Collectors.toList());
          response.setCommits(commits);
        } else {
          response.setCommits(List.of());
        }
        response.setLatestTableVersion(commitsResponse.getLatestTableVersion());
      } catch (Exception e) {
        // If commits can't be retrieved, just set defaults
        response.setCommits(List.of());
        response.setLatestTableVersion(-1L);
      }
    } else {
      response.setCommits(List.of());
      response.setLatestTableVersion(-1L);
    }

    response.setConfig(Map.of());

    // Add credentials if requested
    if (withCredentials && tableInfo.getStorageLocation() != null) {
      NormalizedURL storageLocation = NormalizedURL.from(tableInfo.getStorageLocation());
      TemporaryCredentials credentials =
          storageCredentialVendor.vendCredential(storageLocation, Set.of(SELECT, UPDATE));
      response.setStorageCredentials(
          buildStorageCredentials(tableInfo.getStorageLocation(), credentials));
    }

    return response;
  }

  private List<StorageCredential> buildStorageCredentials(
      String prefix, TemporaryCredentials credentials) {
    StorageCredential credential = new StorageCredential();
    credential.setPrefix(prefix);

    Map<String, String> config = new HashMap<>();
    if (credentials.getAwsTempCredentials() != null) {
      var aws = credentials.getAwsTempCredentials();
      if (aws.getAccessKeyId() != null) {
        config.put("s3.access-key-id", aws.getAccessKeyId());
      }
      if (aws.getSecretAccessKey() != null) {
        config.put("s3.secret-access-key", aws.getSecretAccessKey());
      }
      if (aws.getSessionToken() != null) {
        config.put("s3.session-token", aws.getSessionToken());
      }
    }
    if (credentials.getAzureUserDelegationSas() != null) {
      var azure = credentials.getAzureUserDelegationSas();
      if (azure.getSasToken() != null) {
        config.put("azure.sas-token", azure.getSasToken());
      }
    }
    if (credentials.getGcpOauthToken() != null) {
      var gcp = credentials.getGcpOauthToken();
      if (gcp.getOauthToken() != null) {
        config.put("gcs.oauth-token", gcp.getOauthToken());
      }
    }
    credential.setConfig(config);

    // Add expiration at top level to standardize across all cloud providers
    if (credentials.getExpirationTime() != null) {
      credential.setExpirationTimeMs(credentials.getExpirationTime());
    }

    return List.of(credential);
  }

  private DeltaColumn convertColumnToDeltaColumn(io.unitycatalog.server.model.ColumnInfo column) {
    // Build StructField for type-json
    StructField typeJson = new StructField();
    typeJson.setName(column.getName());

    // Convert ColumnTypeName to Delta/Spark JSON type name
    String jsonTypeName = "string"; // default
    if (column.getTypeName() != null) {
      jsonTypeName = switch (column.getTypeName()) {
        case INT -> "integer";
        case LONG -> "long";
        case SHORT -> "short";
        case BYTE -> "byte";
        case FLOAT -> "float";
        case DOUBLE -> "double";
        case BOOLEAN -> "boolean";
        case STRING -> "string";
        case BINARY -> "binary";
        case DATE -> "date";
        case TIMESTAMP -> "timestamp";
        case TIMESTAMP_NTZ -> "timestamp_ntz";
        case DECIMAL -> column.getTypeText(); // Use typeText for complex types like DECIMAL(10,2)
        case ARRAY, MAP, STRUCT -> column.getTypeText(); // Use typeText for complex types
        default -> "string";
      };
    }

    typeJson.setType(jsonTypeName);
    typeJson.setNullable(column.getNullable() != null ? column.getNullable() : true);
    typeJson.setComment(column.getComment());
    typeJson.setMetadata(Map.of());

    // DeltaColumn only contains typeJson - all other fields are derived from it
    DeltaColumn deltaColumn = new DeltaColumn();
    deltaColumn.setTypeJson(typeJson);

    return deltaColumn;
  }

  private io.unitycatalog.server.model.deltarest.DeltaCommit convertCommitInfoToDeltaCommit(
      io.unitycatalog.server.model.DeltaCommitInfo commitInfo) {
    io.unitycatalog.server.model.deltarest.DeltaCommit deltaCommit =
        new io.unitycatalog.server.model.deltarest.DeltaCommit();
    deltaCommit.setVersion(commitInfo.getVersion());
    deltaCommit.setTimestamp(commitInfo.getTimestamp());
    deltaCommit.setFileName(commitInfo.getFileName());
    deltaCommit.setFileSize(commitInfo.getFileSize());
    deltaCommit.setFileModificationTimestamp(commitInfo.getFileModificationTimestamp());
    return deltaCommit;
  }

  public void filterTables(String expression, List<TableInfo> entries) {
    if (entries == null) return;
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        ti -> {
          CatalogInfo catalogInfo = catalogRepository.getCatalog(ti.getCatalogName());
          SchemaInfo schemaInfo =
              schemaRepository.getSchema(ti.getCatalogName() + "." + ti.getSchemaName());
          return Map.of(
              METASTORE,
              metastoreRepository.getMetastoreId(),
              CATALOG,
              UUID.fromString(catalogInfo.getId()),
              SCHEMA,
              UUID.fromString(schemaInfo.getSchemaId()),
              TABLE,
              UUID.fromString(ti.getTableId()));
        });
  }

  public void filterSchemas(String expression, List<SchemaInfo> entries) {
    if (entries == null) return;
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        si -> {
          CatalogInfo catalogInfo = catalogRepository.getCatalog(si.getCatalogName());
          return Map.of(
              METASTORE,
              metastoreRepository.getMetastoreId(),
              CATALOG,
              UUID.fromString(catalogInfo.getId()),
              SCHEMA,
              UUID.fromString(si.getSchemaId()));
        });
  }
}
