package io.unitycatalog.spark

import io.unitycatalog.client.api.TablesApi
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.deltarest.api.{ConfigurationApi => DeltaRestConfigurationApi, NamespacesApi => DeltaRestNamespacesApi, TablesApi => DeltaRestTablesApi}
import io.unitycatalog.client.deltarest.model.{CatalogConfig => DeltaRestCatalogConfig, CreateNamespaceRequest => DeltaRestCreateNamespaceRequest, CreateStagingTableRequest => DeltaRestCreateStagingTableRequest, CreateTableRequest => DeltaRestCreateTableRequest, DeltaColumn => DeltaRestDeltaColumn, DeltaProtocol => DeltaRestDeltaProtocol, DataSourceFormat => DeltaRestDataSourceFormat, ListNamespacesResponse => DeltaRestListNamespacesResponse, ListTablesResponse => DeltaRestListTablesResponse, LoadTableResponse => DeltaRestLoadTableResponse, NamespaceResponse => DeltaRestNamespaceResponse, StagingTableResponse => DeltaRestStagingTableResponse, StorageCredential => DeltaRestStorageCredential, StructField => DeltaRestStructField, TableIdentifierWithDataSourceFormat => DeltaRestTableIdentifierWithDataSourceFormat, TableMetadata => DeltaRestTableMetadata}
import io.unitycatalog.client.deltarest.{ApiClient => DeltaRestApiClient}
import io.unitycatalog.client.model.{ColumnInfo, ColumnTypeName, CreateTable, DataSourceFormat, TableType}
import io.unitycatalog.client.retry.JitterDelayRetryPolicy
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.spark.auth.{AuthConfigUtils, CredPropsUtil}
import io.unitycatalog.spark.utils.OptionsUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.sparkproject.guava.base.Preconditions

import java.net.URI
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.language.existentials

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog
  extends TableCatalog
  with SupportsNamespaces
  with Logging {

  private[this] var uri: URI = null
  private[this] var tokenProvider: TokenProvider = null
  private[this] var renewCredEnabled: Boolean = false
  private[this] var apiClient: ApiClient = null
  private[this] var deltaRestConfigurationApi: DeltaRestConfigurationApi = null
  private[this] var deltaRestTablesApi: DeltaRestTablesApi = null
  private[this] var catalogConfig: DeltaRestCatalogConfig = null

  @volatile private var delegate: TableCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val urlStr = options.get(OptionsUtil.URI)
    Preconditions.checkArgument(urlStr != null,
      "uri must be specified for Unity Catalog '%s'", name)
    uri = new URI(urlStr)
    tokenProvider = TokenProvider.create(AuthConfigUtils.buildAuthConfigs(options));
    renewCredEnabled = OptionsUtil.getBoolean(options,
      OptionsUtil.RENEW_CREDENTIAL_ENABLED,
      OptionsUtil.DEFAULT_RENEW_CREDENTIAL_ENABLED)

    // Create Unity Catalog API client for non-Delta/non-Iceberg tables
    apiClient = ApiClientFactory.createApiClient(
      JitterDelayRetryPolicy.builder().build(), uri, tokenProvider)

    // Create Delta REST API client
    val deltaRestUri = uri.toString + "/api/2.1/unity-catalog/delta-rest/v1"
    val deltaRestApiClient = new DeltaRestApiClient()
    deltaRestApiClient.updateBaseUri(deltaRestUri)
    deltaRestConfigurationApi = new DeltaRestConfigurationApi(deltaRestApiClient)
    deltaRestTablesApi = new DeltaRestTablesApi(deltaRestApiClient)

    // Get catalog configuration from Delta REST API
    try {
      catalogConfig = deltaRestConfigurationApi.getConfig(name)
      logInfo(s"Delta REST Catalog configured with ${catalogConfig.getEndpoints.size()} endpoints")
    } catch {
      case e: Exception =>
        logWarning(s"Failed to get Delta REST catalog config: ${e.getMessage}")
    }

    val proxy = new UCProxy(uri, tokenProvider, renewCredEnabled, apiClient)
    proxy.initialize(name, options)
    if (UCSingleCatalog.LOAD_DELTA_CATALOG.get()) {
      try {
        delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
        delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
        delegate.initialize(name, options)
        UCSingleCatalog.DELTA_CATALOG_LOADED.set(true)
      } catch {
        case e: ClassNotFoundException =>
          logWarning("DeltaCatalog is not available in the classpath", e)
          delegate = proxy
      }
    } else {
      delegate = proxy
    }
  }

  override def name(): String = delegate.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = delegate.listTables(namespace)

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident)

  override def loadTable(ident: Identifier, version:  String): Table = delegate.loadTable(ident, version)

  override def loadTable(ident: Identifier, timestamp:  Long): Table = delegate.loadTable(ident, timestamp)

  override def tableExists(ident: Identifier): Boolean = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    delegate.tableExists(ident)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION)
    if (hasExternalClause && !hasLocationClause) {
      throw new ApiException("Cannot create EXTERNAL TABLE without location.")
    }
    def isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute

    // If both EXTERNAL and LOCATION are not specified in the CREATE TABLE command, and the table is
    // not a path table like parquet.`/file/path`, we generate the UC-managed table location here.
    if (!hasExternalClause && !hasLocationClause && !isPathTable) {
      // Check that caller shouldn't set some properties
      List(UCTableProperties.UC_TABLE_ID_KEY, UCTableProperties.UC_TABLE_ID_KEY_OLD,
        TableCatalog.PROP_IS_MANAGED_LOCATION)
        .filter(properties.containsKey(_))
        .foreach(p => throw new ApiException(s"Cannot specify property '$p'."))
      // Setting the catalogManaged table feature is required for creating a managed table.
      if (!properties.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY) &&
        !properties.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)) {
        throw new ApiException(
          s"Managed table creation requires table property " +
            s"'${UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW}'=" +
            s"'${UCTableProperties.DELTA_CATALOG_MANAGED_VALUE}'" +
            s" to be set.")
      }
      // Caller should not set these two table properties to values other than "supported". This is
      // the only documented value.
      List(UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
        UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)
        .foreach(k => {
          Option(properties.get(k))
            .filter(_ != UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
            .foreach(v => throw new ApiException(
              s"Invalid property value '$v' for '$k'."))
        })

      // Get staging table location and table id from Delta REST API
      val createStagingTableRequest = new DeltaRestCreateStagingTableRequest()
      createStagingTableRequest.setName(ident.name())
      val stagingTableResponse: DeltaRestStagingTableResponse =
        deltaRestTablesApi.createStagingTable(name(), ident.namespace().head, createStagingTableRequest)
      val stagingLocation = stagingTableResponse.getLocation
      val stagingTableId = stagingTableResponse.getTableId.toString

      val newProps = new util.HashMap[String, String]
      newProps.putAll(properties)
      newProps.put(TableCatalog.PROP_LOCATION, stagingLocation)
      // Sets both the new and old table ID property while it's being renamed.
      newProps.put(UCTableProperties.UC_TABLE_ID_KEY, stagingTableId)
      newProps.put(UCTableProperties.UC_TABLE_ID_KEY_OLD, stagingTableId)
      // `PROP_IS_MANAGED_LOCATION` is used to indicate that the table location is not
      // user-specified but system-generated, which is exactly the case here.
      newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")

      // Add Delta table feature requirements from catalog config
      // Only apply Delta features to Delta tables
      val provider = Option(properties.get(TableCatalog.PROP_PROVIDER)).getOrElse("delta")
      if (provider.equalsIgnoreCase("delta")) {
        // Each feature is added as a separate property: delta.feature.X=supported
        // Filter out features that aren't supported in all Delta versions
        val unsupportedFeatures = Set("catalogManaged", "catalogmanaged")
        if (catalogConfig != null && catalogConfig.getManagedTablesRequiredFeatures != null) {
          catalogConfig.getManagedTablesRequiredFeatures.asScala
            .filterNot(f => unsupportedFeatures.contains(f))
            .foreach { feature =>
              newProps.put(s"delta.feature.$feature", "supported")
            }
        }
        // Suggested features are not yet implemented
        // if (catalogConfig != null && catalogConfig.getManagedTablesSuggestedFeatures != null) {
        //   catalogConfig.getManagedTablesSuggestedFeatures.asScala
        //     .filterNot(f => unsupportedFeatures.contains(f))
        //     .foreach { feature =>
        //       newProps.put(s"delta.feature.$feature", "supported")
        //     }
        // }
      }

      // Use storage credentials from staging table response
      val storageCredentials = stagingTableResponse.getStorageCredentials
      if (storageCredentials != null && !storageCredentials.isEmpty) {
        val cred = storageCredentials.get(0)
        val locationUri = new URI(stagingLocation)
        val scheme = locationUri.getScheme
        // Use CredPropsUtil to get proper Hadoop config
        val credProps = io.unitycatalog.spark.auth.CredPropsUtil.createTableCredProps(
          renewCredEnabled,
          scheme,
          uri.toString,
          tokenProvider,
          stagingTableId,
          io.unitycatalog.client.model.TableOperation.UNKNOWN_TABLE_OPERATION,
          cred
        )
        credProps.asScala.foreach { case (key, value) =>
          newProps.put(key, value)
          newProps.put(TableCatalog.OPTION_PREFIX + key, value)
        }
      }

      delegate.createTable(ident, columns, partitions, newProps)
    } else if (hasLocationClause) {
      // For external tables with user-provided location, get credentials for the location
      val location = properties.get(TableCatalog.PROP_LOCATION)
      val newProps = new util.HashMap[String, String]
      newProps.putAll(properties)

      // Get credentials for the external location from Delta REST API
      try {
        val response = deltaRestTablesApi.getTemporaryPathCredentials(
          location,
          "PATH_CREATE_TABLE"
        )
        val storageCredentials = response.getStorageCredentials
        if (storageCredentials != null && !storageCredentials.isEmpty) {
          val cred = storageCredentials.get(0)
          val locationUri = new URI(location)
          val scheme = locationUri.getScheme
          // Generate a temporary table ID for credential tracking
          val tempTableId = java.util.UUID.randomUUID().toString
          val credProps = io.unitycatalog.spark.auth.CredPropsUtil.createTableCredProps(
            renewCredEnabled,
            scheme,
            uri.toString,
            tokenProvider,
            tempTableId,
            io.unitycatalog.client.model.TableOperation.READ_WRITE,
            cred
          )
          credProps.asScala.foreach { case (key, value) =>
            newProps.put(key, value)
            newProps.put(TableCatalog.OPTION_PREFIX + key, value)
          }
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to get credentials for external table location $location: ${e.getMessage}")
      }

      delegate.createTable(ident, columns, partitions, newProps)
    } else {
      // TODO: for path-based tables, Spark should generate a location property using the qualified
      //       path string.
      delegate.createTable(ident, columns, partitions, properties)
    }
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    throw new AssertionError("deprecated `createTable` should not be called")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    delegate.dropTable(ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  override def listNamespaces(): Array[Array[String]] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].loadNamespaceMetadata(namespace)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    delegate.asInstanceOf[DelegatingCatalogExtension].createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    delegate.asInstanceOf[DelegatingCatalogExtension].alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    delegate.asInstanceOf[DelegatingCatalogExtension].dropNamespace(namespace, cascade)
  }
}

object UCSingleCatalog {
  val LOAD_DELTA_CATALOG = ThreadLocal.withInitial[Boolean](() => true)
  val DELTA_CATALOG_LOADED = ThreadLocal.withInitial[Boolean](() => false)

  def setCredentialProps(props: util.HashMap[String, String],
                         credentialProps: util.Map[String, String]): Unit = {
    props.putAll(credentialProps)
    // TODO: Delta requires the options to be set twice in the properties, with and without the
    //       `option.` prefix. We should revisit this in Delta.
    val prefix = TableCatalog.OPTION_PREFIX
    props.putAll(credentialProps.map {
      case (k, v) => (prefix + k, v)
    }.asJava)
  }

  def checkUnsupportedNestedNamespace(namespace: Array[String]): Unit = {
    if (namespace.length > 1) {
      throw new ApiException("Nested namespaces are not supported: " + namespace.mkString("."))
    }
  }

  /**
   * Constructs a fully qualified table name for Unity Catalog API calls.
   *
   * This method creates a three-part name in the format `catalog.schema.table` by combining
   * the catalog name with the schema name (from the identifier's namespace) and table name.
   * It is NOT backtick quoted like what is usually used in SQL statements even if the names have
   * special characters like hyphens.
   *
   * Example:
   * catalogName=catalog, ident=(schema, table): it returns "catalog.schema.table"
   * catalogName=cata-log, ident=(sche-ma, ta-ble): it returns "cata-log.sche-ma.ta-ble" (no quote)
   * catalogName=catalog, ident=((schema1, schema2), table): it throws
   *   ApiException(Nested namespace not supported)
   *
   * @param catalogName the name of the catalog
   * @param ident the table identifier containing the namespace (schema) and table name
   * @return a fully qualified table name in the format "catalog.schema.table"
   * @throws ApiException if the identifier contains nested namespaces (more than one level)
   */
  def fullTableNameForApi(catalogName: String, ident: Identifier): String = {
    checkUnsupportedNestedNamespace(ident.namespace())
    Seq(catalogName, ident.namespace()(0), ident.name()).mkString(".")
  }
}

// An internal proxy to talk to the UC client.
private class UCProxy(
    uri: URI,
    tokenProvider: TokenProvider,
    renewCredEnabled: Boolean,
    apiClient: ApiClient) extends TableCatalog with SupportsNamespaces {
  private[this] var name: String = null
  private[this] var tablesApi: TablesApi = null
  private[this] var deltaRestTablesApi: DeltaRestTablesApi = null
  private[this] var deltaRestNamespacesApi: DeltaRestNamespacesApi = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.name = name

    // Create Unity Catalog TablesApi for non-Delta/non-Iceberg tables
    tablesApi = new TablesApi(apiClient)

    // Create Delta REST API client with Delta REST endpoint
    val deltaRestUri = uri.toString + "/api/2.1/unity-catalog/delta-rest/v1"
    val deltaRestApiClient = new DeltaRestApiClient()
    deltaRestApiClient.updateBaseUri(deltaRestUri)
    deltaRestTablesApi = new DeltaRestTablesApi(deltaRestApiClient)
    deltaRestNamespacesApi = new DeltaRestNamespacesApi(deltaRestApiClient)
  }

  override def name(): String = {
    assert(this.name != null)
    this.name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)

    val catalogName = this.name
    val schemaName = namespace.head
    val pageToken = null
    val response: DeltaRestListTablesResponse = deltaRestTablesApi.listTables(catalogName, schemaName, pageToken)
    response.getIdentifiers.asScala.map(tableIdent => Identifier.of(namespace, tableIdent.getName)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val response = try {
      deltaRestTablesApi.loadTable(
        this.name,
        ident.namespace().head,
        ident.name(),
        /* withCredentials = */ true)
    } catch {
      case e: io.unitycatalog.client.deltarest.ApiException if e.getCode == 404 =>
        throw new NoSuchTableException(ident)
    }

    val metadata = response.getMetadata
    val identifier = TableIdentifier(ident.name(), Some(ident.namespace().head), Some(this.name))
    val partitionCols = scala.collection.mutable.ArrayBuffer.empty[(String, Int)]

    // Convert Delta REST schema to Spark schema
    val fields = metadata.getSchema.asScala.zipWithIndex.map { case (deltaCol, index) =>
      val structField = deltaCol.getTypeJson
      // Serialize type object to JSON string for Spark's DataType.fromJson
      // For primitive types, we need to wrap the type name in quotes to make it valid JSON
      val typeJson = structField.getType match {
        case s: String =>
          // Wrap string in quotes to make it valid JSON: "string" becomes "\"string\""
          s"""\"$s\""""
        case obj =>
          // Complex types are already objects, serialize them as-is
          new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(obj)
      }
      val dataType = DataType.fromJson(typeJson)
      // Check if this is a partition column (assuming partition columns are marked in properties)
      StructField(structField.getName, dataType, structField.getNullable)
    }.toArray

    val locationUri = CatalogUtils.stringToURI(metadata.getLocation)
    val tableId = metadata.getTableUuid.toString

    // Get storage credentials from the response
    val storageCredentials = response.getStorageCredentials
    val extraSerdeProps = if (storageCredentials != null && !storageCredentials.isEmpty) {
      val cred = storageCredentials.get(0)
      val scheme = locationUri.getScheme
      // Use CredPropsUtil to get proper Hadoop config
      val credProps = io.unitycatalog.spark.auth.CredPropsUtil.createTableCredProps(
        renewCredEnabled,
        scheme,
        uri.toString,
        tokenProvider,
        tableId,
        io.unitycatalog.client.model.TableOperation.READ,
        cred
      )
      credProps.asScala.toMap
    } else {
      Map.empty[String, String]
    }

    val sparkTable = CatalogTable(
      identifier,
      tableType = if (metadata.getTableType == DeltaRestTableMetadata.TableTypeEnum.MANAGED) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      },
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(locationUri),
        properties = metadata.getProperties.asScala.toMap ++ extraSerdeProps
      ),
      schema = StructType(fields),
      provider = Some(metadata.getDataSourceFormat.getValue.toLowerCase()),
      createTime = metadata.getCreateTime,
      tracksPartitionsInCatalog = false,
      partitionColumnNames = partitionCols.sortBy(_._2).map(_._1).toSeq
    )
    // Spark separates table lookup and data source resolution. To support Spark native data
    // sources, here we return the `V1Table` which only contains the table metadata. Spark will
    // resolve the data source and create scan node later.
    Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
      .getDeclaredConstructor(classOf[CatalogTable])
      .newInstance(sparkTable)
      .asInstanceOf[Table]
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    assert(properties.get(TableCatalog.PROP_PROVIDER) != null)

    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val storageLocation = properties.get(TableCatalog.PROP_LOCATION)
    assert(storageLocation != null, "location should either be user specified or system generated.")
    val isManagedLocation = Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
      .exists(_.equalsIgnoreCase("true"))
    val format = properties.get("provider")

    // Determine table type
    val tableType = if (isManagedLocation) {
      assert(!hasExternalClause, "location is only generated for managed tables.")
      if (!format.equalsIgnoreCase("DELTA")) {
        throw new ApiException("Unity Catalog does not support non-Delta managed table.")
      }
      DeltaRestCreateTableRequest.TableTypeEnum.MANAGED
    } else {
      DeltaRestCreateTableRequest.TableTypeEnum.EXTERNAL
    }

    // Convert schema to Delta REST format
    val deltaColumns: Seq[DeltaRestDeltaColumn] = schema.fields.toSeq.zipWithIndex.map { case (field, i) =>
      val structField = new DeltaRestStructField()
      structField.setName(field.name)
      // Use Spark's JSON representation for the type
      // For primitive types, Spark's json property returns a quoted string like "\"integer\""
      // We need to strip the quotes to get just the type name
      val jsonType = field.dataType.json
      val typeValue = if (jsonType.startsWith("\"") && jsonType.endsWith("\"")) {
        // Remove surrounding quotes for primitive types
        jsonType.substring(1, jsonType.length - 1)
      } else {
        // Complex types (structs, arrays, maps) - use as-is
        jsonType
      }
      structField.setType(typeValue)
      structField.setNullable(field.nullable)
      if (field.getComment().isDefined) {
        structField.setComment(field.getComment.get)
      }
      structField.setMetadata(new java.util.HashMap[String, AnyRef]())

      val deltaColumn = new DeltaRestDeltaColumn()
      deltaColumn.setTypeJson(structField)
      deltaColumn
    }

    // Create Delta protocol
    val protocol = new DeltaRestDeltaProtocol()
    protocol.setMinReaderVersion(1)
    protocol.setMinWriterVersion(2)

    // Create Delta REST request
    val createTableRequest = new DeltaRestCreateTableRequest()
    createTableRequest.setName(ident.name())
    createTableRequest.setLocation(storageLocation)
    createTableRequest.setTableType(tableType)
    createTableRequest.setDataSourceFormat(convertDatasourceFormatToDeltaRest(format))
    createTableRequest.setSchema(deltaColumns.asJava)
    createTableRequest.setProtocol(protocol)

    // Set comment if present in properties
    val comment = properties.get(TableCatalog.PROP_COMMENT)
    if (comment != null) {
      createTableRequest.setComment(comment)
    }

    // Do not send the V2 table properties as they are made part of the `createTable` already.
    val propertiesToServer =
      properties.view.filterKeys(!UCTableProperties.V2_TABLE_PROPERTIES.contains(_)).toMap
    createTableRequest.setProperties(propertiesToServer.asJava)

    deltaRestTablesApi.createTable(this.name, ident.namespace().head, createTableRequest)
    loadTable(ident)
  }

  private def convertDatasourceFormat(format: String): DataSourceFormat = {
    format.toUpperCase match {
      case "PARQUET" => DataSourceFormat.PARQUET
      case "CSV" => DataSourceFormat.CSV
      case "DELTA" => DataSourceFormat.DELTA
      case "JSON" => DataSourceFormat.JSON
      case "ORC" => DataSourceFormat.ORC
      case "TEXT" => DataSourceFormat.TEXT
      case "AVRO" => DataSourceFormat.AVRO
      case _ => throw new ApiException("DataSourceFormat not supported: " + format)
    }
  }

  private def convertDatasourceFormatToDeltaRest(format: String): DeltaRestDataSourceFormat = {
    format.toUpperCase match {
      case "DELTA" => DeltaRestDataSourceFormat.DELTA
      case "ICEBERG" => DeltaRestDataSourceFormat.ICEBERG
      case "PARQUET" => DeltaRestDataSourceFormat.PARQUET
      case "CSV" => DeltaRestDataSourceFormat.CSV
      case "JSON" => DeltaRestDataSourceFormat.JSON
      case "ORC" => DeltaRestDataSourceFormat.ORC
      case "TEXT" => DeltaRestDataSourceFormat.TEXT
      case "AVRO" => DeltaRestDataSourceFormat.AVRO
      case _ => throw new ApiException("DataSourceFormat not supported for Delta REST: " + format)
    }
  }

  private def convertDataTypeToTypeName(dataType: DataType): ColumnTypeName = {
    dataType match {
      case StringType => ColumnTypeName.STRING
      case BooleanType => ColumnTypeName.BOOLEAN
      case ShortType => ColumnTypeName.SHORT
      case IntegerType => ColumnTypeName.INT
      case LongType => ColumnTypeName.LONG
      case FloatType => ColumnTypeName.FLOAT
      case DoubleType => ColumnTypeName.DOUBLE
      case ByteType => ColumnTypeName.BYTE
      case BinaryType => ColumnTypeName.BINARY
      case TimestampNTZType => ColumnTypeName.TIMESTAMP_NTZ
      case TimestampType => ColumnTypeName.TIMESTAMP
      case _ => throw new ApiException("DataType not supported: " + dataType.simpleString)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    deltaRestTablesApi.deleteTable(this.name, ident.namespace().head, ident.name())
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  override def listNamespaces(): Array[Array[String]] = {
    val response: DeltaRestListNamespacesResponse = deltaRestNamespacesApi.listNamespaces(name, null)
    response.getNamespaces.asScala.map { ns =>
      ns.asScala.toArray
    }.toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    throw new UnsupportedOperationException("Multi-layer namespace is not supported in Unity Catalog")
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    val response = try {
      deltaRestNamespacesApi.loadNamespace(name, namespace(0))
    } catch {
      case e: io.unitycatalog.client.deltarest.ApiException if e.getCode == 404 =>
        throw new NoSuchNamespaceException(namespace)
    }
    // Return namespace properties
    response.getProperties.asScala.toMap.asJava
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    val createRequest = new DeltaRestCreateNamespaceRequest()
    createRequest.setNamespace(namespace.toSeq.asJava)
    createRequest.setProperties(metadata)
    deltaRestNamespacesApi.createNamespace(name, createRequest)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("Renaming a namespace is not supported yet")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    deltaRestNamespacesApi.deleteNamespace(name, namespace.head)
    true
  }
}
