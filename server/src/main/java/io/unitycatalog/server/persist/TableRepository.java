package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.deltarest.AddCommitUpdate;
import io.unitycatalog.server.model.deltarest.DeltaColumn;
import io.unitycatalog.server.model.deltarest.RemovePropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetLatestBackfilledVersionUpdate;
import io.unitycatalog.server.model.deltarest.SetPropertiesUpdate;
import io.unitycatalog.server.model.deltarest.SetSchemaUpdate;
import io.unitycatalog.server.model.deltarest.SetTableCommentUpdate;
import io.unitycatalog.server.model.deltarest.StructField;
import io.unitycatalog.server.model.deltarest.TableRequirement;
import io.unitycatalog.server.model.deltarest.UpdateProtocolUpdate;
import io.unitycatalog.server.model.deltarest.UpdateTableRequest;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private final FileOperations fileOperations;
  private final ServerProperties serverProperties;
  private static final PagedListingHelper<TableInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(TableInfoDAO.class);

  public TableRepository(
      Repositories repositories, SessionFactory sessionFactory, ServerProperties serverProperties) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.fileOperations = repositories.getFileOperations();
    this.serverProperties = serverProperties;
  }

  /**
   * Retrieves the storage location for a table or staging table by its ID. First attempts to find a
   * regular table with the given ID, then falls back to searching for a staging table if no regular
   * table is found. NOTE: This function is specially needed by generateTemporaryTableCredential
   * during the short window when a staging table is just created and the initial data is being
   * written but before the actual table is already created. Reading of a staging table is not a
   * common supplemental of an actual table but only a special case.
   *
   * @param tableId the ID of the table or staging table
   * @return the normalized URL of the storage location
   * @throws BaseException with ErrorCode.NOT_FOUND if neither a table nor staging table is found
   *     with the given ID
   */
  public NormalizedURL getStorageLocationForTableOrStagingTable(UUID tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of table by id: {}", tableId);
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);
          if (tableInfoDAO != null) {
            return NormalizedURL.from(tableInfoDAO.getUrl());
          }

          LOGGER.debug("Getting storage location of staging table by id: {}", tableId);
          StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
          if (stagingTableDAO != null) {
            return NormalizedURL.from(stagingTableDAO.getStagingLocation());
          }
          throw new BaseException(
              ErrorCode.NOT_FOUND, "Neither table nor staging table found with id: " + tableId);
        },
        "Failed to get storage location of table or staging table",
        /* readOnly = */ true);
  }

  public NormalizedURL getStorageLocationForStagingTable(UUID tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of staging table by id: {}", tableId);
          StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
          if (stagingTableDAO != null) {
            return NormalizedURL.from(stagingTableDAO.getStagingLocation());
          }
          throw new BaseException(
              ErrorCode.NOT_FOUND, "No staging table found with id: " + tableId);
        },
        "Failed to get storage location of staging table",
        /* readOnly = */ true);
  }

  /**
   * Retrieves the schema ID and catalog ID for a table or staging table by its ID. First attempts
   * to get IDs associated with a regular table with the given ID, then falls back to searching for
   * a staging table if no regular table is found. NOTE: Similar to
   * getStorageLocationForTableOrStagingTable, this function is specially needed by KeyMapper during
   * authorization of generateTemporaryTableCredential. Reading of a staging table is not a common
   * supplemental of an actual table but only a special case.
   *
   * @param tableId the UUID of the table or staging table
   * @return a Pair containing the catalog ID (left) and schema ID (right)
   * @throws BaseException with ErrorCode.NOT_FOUND if neither a table nor staging table is found
   *     with the given ID, or if the associated schema is not found
   */
  public Pair<UUID, UUID> getCatalogSchemaIdsByTableOrStagingTableId(UUID tableId) {
    LOGGER.debug("Getting catalog&schema id by table or staging table id: {}", tableId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableId);

          UUID schemaId;
          if (tableInfoDAO != null) {
            schemaId = tableInfoDAO.getSchemaId();
          } else {
            // Table not found, try to find a staging table instead
            StagingTableDAO stagingTableDAO = session.get(StagingTableDAO.class, tableId);
            if (stagingTableDAO == null) {
              throw new BaseException(
                  ErrorCode.NOT_FOUND, "Neither table nor staging table found with id: " + tableId);
            }
            schemaId = stagingTableDAO.getSchemaId();
          }

          SchemaInfoDAO schemaInfoDAO = session.get(SchemaInfoDAO.class, schemaId);
          if (schemaInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found with id: " + schemaId);
          }

          return Pair.of(schemaInfoDAO.getCatalogId(), schemaId);
        },
        "Failed to get table or staging table by ID",
        /* readOnly = */ true);
  }

  public TableInfo getTable(String fullName) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> getTable(session, fullName),
        "Failed to get table",
        /* readOnly = */ true);
  }

  /**
   * Session-aware version of getTable for use within an existing transaction.
   *
   * @param session the Hibernate session
   * @param fullName the full table name (catalog.schema.table)
   * @return the TableInfo
   */
  public TableInfo getTable(Session session, String fullName) {
    LOGGER.debug("Getting table: {}", fullName);
    String[] parts = fullName.split("\\.");
    if (parts.length != 3) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
    }
    String catalogName = parts[0];
    String schemaName = parts[1];
    String tableName = parts[2];

    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);

    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + fullName);
    }

    TableInfo result = tableInfoDAO.toTableInfo(true, catalogName, schemaName);
    RepositoryUtils.attachProperties(result, result.getTableId(), Constants.TABLE, session);
    return result;
  }

  public String getTableUniformMetadataLocation(
      Session session, String catalogName, String schemaName, String tableName) {
    TableInfoDAO dao = findTable(session, catalogName, schemaName, tableName);
    return dao.getUniformIcebergMetadataLocation();
  }

  private TableInfoDAO findTable(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return findBySchemaIdAndName(session, schemaId, tableName);
  }

  public TableInfo createTable(CreateTable createTable) {
    ValidationUtils.validateSqlObjectName(createTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    List<ColumnInfo> columnInfos =
        createTable.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());
    Long createTime = System.currentTimeMillis();
    String fullName = getTableFullName(createTable);
    LOGGER.debug("Creating table: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = createTable.getCatalogName();
          String schemaName = createTable.getSchemaName();
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          NormalizedURL storageLocation = NormalizedURL.from(createTable.getStorageLocation());

          // Check if table already exists
          TableInfoDAO existingTable =
              findBySchemaIdAndName(session, schemaId, createTable.getName());
          if (existingTable != null) {
            throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + fullName);
          }
          TableType tableType = Objects.requireNonNull(createTable.getTableType());
          // The table ID will either be a new random one or the id of staging table, depending
          // on the type of table to create.
          String tableID;
          if (tableType == TableType.EXTERNAL) {
            tableID = UUID.randomUUID().toString();
          } else if (tableType == TableType.MANAGED) {
            serverProperties.checkManagedTableEnabled();
            if (createTable.getDataSourceFormat() != DataSourceFormat.DELTA) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Managed table creation is only supported for Delta format.");
            }
            // Find and commit staging table with the same staging location
            StagingTableDAO stagingTableDAO =
                repositories
                    .getStagingTableRepository()
                    .commitStagingTable(session, callerId, storageLocation);
            tableID = stagingTableDAO.getId().toString();
          } else if (tableType == TableType.STREAMING_TABLE) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT, "STREAMING TABLE creation is not supported yet.");
          } else if (tableType == TableType.MATERIALIZED_VIEW) {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT, "MATERIALIZED VIEW creation is not supported yet.");
          } else {
            throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                "Unrecognized table type " + createTable.getTableType());
          }
          TableInfo tableInfo =
              new TableInfo()
                  .name(createTable.getName())
                  .catalogName(createTable.getCatalogName())
                  .schemaName(createTable.getSchemaName())
                  .tableType(createTable.getTableType())
                  .dataSourceFormat(createTable.getDataSourceFormat())
                  .columns(columnInfos)
                  .comment(createTable.getComment())
                  .properties(createTable.getProperties())
                  .owner(callerId)
                  .createdAt(createTime)
                  .createdBy(callerId)
                  .updatedAt(createTime)
                  .updatedBy(callerId)
                  .storageLocation(storageLocation.toString())
                  .tableId(tableID);

          TableInfoDAO tableInfoDAO = TableInfoDAO.from(tableInfo, schemaId);
          // create columns
          tableInfoDAO
              .getColumns()
              .forEach(
                  c -> {
                    c.setId(UUID.randomUUID());
                    c.setTable(tableInfoDAO);
                  });
          // create properties
          PropertyDAO.from(tableInfo.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::persist);
          session.persist(tableInfoDAO);
          return tableInfo;
        },
        "Error creating table: " + fullName,
        /* readOnly = */ false);
  }

  public TableInfoDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql = "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding table by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private String getTableFullName(CreateTable createTable) {
    return createTable.getCatalogName()
        + "."
        + createTable.getSchemaName()
        + "."
        + createTable.getName();
  }

  /**
   * Return the list of tables in ascending order of table name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @param omitProperties
   * @param omitColumns
   * @return
   */
  public ListTablesResponse listTables(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          return listTables(
              session,
              schemaId,
              catalogName,
              schemaName,
              maxResults,
              pageToken,
              omitProperties,
              omitColumns);
        },
        "Failed to list tables",
        /* readOnly = */ true);
  }

  public ListTablesResponse listTables(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    List<TableInfoDAO> tableInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(tableInfoDAOList, maxResults);
    List<TableInfo> result = new ArrayList<>();
    for (TableInfoDAO tableInfoDAO : tableInfoDAOList) {
      TableInfo tableInfo = tableInfoDAO.toTableInfo(!omitColumns, catalogName, schemaName);
      if (!omitProperties) {
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
      }
      result.add(tableInfo);
    }
    return new ListTablesResponse().tables(result).nextPageToken(nextPageToken);
  }

  public void deleteTable(String fullName) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String tableName = parts[2];
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          deleteTable(session, schemaId, tableName);
          return null;
        },
        "Failed to delete table",
        /* readOnly = */ false);
  }

  public void deleteTable(Session session, UUID schemaId, String tableName) {
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableName);
    }
    if (TableType.MANAGED.getValue().equals(tableInfoDAO.getType())) {
      try {
        fileOperations.deleteDirectory(tableInfoDAO.getUrl());
      } catch (Throwable e) {
        LOGGER.error("Error deleting table directory: {}", tableInfoDAO.getUrl(), e);
      }
      repositories
          .getDeltaCommitRepository()
          .permanentlyDeleteTableCommits(session, tableInfoDAO.getId());
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }

  /**
   * Updates table metadata including comment, properties, and columns.
   *
   * @param fullName the full table name (catalog.schema.table)
   * @param comment optional new comment for the table
   * @param properties optional properties to set (replaces existing)
   * @param columns optional columns to set (replaces existing)
   * @return the updated TableInfo
   */
  public TableInfo updateTable(
      String fullName, String comment, Map<String, String> properties, List<ColumnInfo> columns) {
    LOGGER.debug("Updating table: {}", fullName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> updateTable(session, fullName, comment, properties, columns),
        "Failed to update table: " + fullName,
        /* readOnly = */ false);
  }

  /**
   * Session-aware version of updateTable for use within an existing transaction.
   *
   * @param session the Hibernate session
   * @param fullName the full table name (catalog.schema.table)
   * @param comment optional new comment for the table
   * @param properties optional properties to set (replaces existing)
   * @param columns optional columns to set (replaces existing)
   * @return the updated TableInfo
   */
  public TableInfo updateTable(
      Session session,
      String fullName,
      String comment,
      Map<String, String> properties,
      List<ColumnInfo> columns) {
    LOGGER.debug("Updating table: {}", fullName);
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    String[] parts = fullName.split("\\.");
    if (parts.length != 3) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
    }
    String catalogName = parts[0];
    String schemaName = parts[1];
    String tableName = parts[2];

    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);

    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, tableName);
    if (tableInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + fullName);
    }

    // Update comment if provided
    if (comment != null) {
      tableInfoDAO.setComment(comment);
    }

    // Update properties if provided
    if (properties != null) {
      PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
          .forEach(session::remove);
      session.flush();
      PropertyDAO.from(properties, tableInfoDAO.getId(), Constants.TABLE).forEach(session::persist);
    }

    // Update columns if provided
    if (columns != null && !columns.isEmpty()) {
      List<ColumnInfoDAO> newColumns = ColumnInfoDAO.fromList(columns);
      tableInfoDAO.getColumns().clear();
      session.flush();
      newColumns.forEach(
          c -> {
            c.setId(UUID.randomUUID());
            c.setTable(tableInfoDAO);
          });
      tableInfoDAO.getColumns().addAll(newColumns);
    }

    tableInfoDAO.setUpdatedBy(callerId);
    tableInfoDAO.setUpdatedAt(new Date());
    session.merge(tableInfoDAO);

    TableInfo result = tableInfoDAO.toTableInfo(true, catalogName, schemaName);
    RepositoryUtils.attachProperties(result, result.getTableId(), Constants.TABLE, session);
    return result;
  }

  /**
   * Updates a table by processing Delta REST requirements and updates in a single transaction. This
   * method handles the complete update flow: get table, validate requirements, process updates,
   * return updated table.
   *
   * @param fullName the full table name (catalog.schema.table)
   * @param request the Delta REST update request containing requirements and updates
   * @return the updated TableInfo
   */
  public TableInfo updateTableWithDeltaRestRequest(String fullName, UpdateTableRequest request) {

    LOGGER.debug("Updating table with Delta REST request: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          // Get table within transaction
          TableInfo info = getTable(session, fullName);

          // Validate requirements before processing updates
          List<TableRequirement> requirements = request.getRequirements();
          if (requirements != null) {
            for (TableRequirement requirement : requirements) {
              validateTableRequirement(info, requirement);
            }
          }

          // Process updates within the same transaction
          // Note: updates are deserialized as concrete types (AddCommitUpdate, etc.)
          // that don't extend TableUpdate, so we use List<?> to avoid ClassCastException
          @SuppressWarnings("unchecked")
          List<Object> updates = (List<Object>) (List<?>) request.getUpdates();
          if (updates != null && !updates.isEmpty()) {
            // Phase 1: Extract and accumulate all updates
            AccumulatedTableUpdates accumulated = accumulateTableUpdates(info, updates);

            // Phase 2: Apply accumulated updates
            processTableUpdates(session, info, fullName, accumulated);
          }

          // Reload table info after updates within same transaction
          return getTable(session, fullName);
        },
        "Failed to update table: " + fullName,
        /* readOnly = */ false);
  }

  /**
   * Accumulates all table updates from a list into a single consolidated structure.
   *
   * @param tableInfo the current table info
   * @param updates the list of updates to accumulate (concrete types like AddCommitUpdate)
   * @return accumulated updates ready for processing
   * @throws BaseException if duplicate update actions are found
   */
  private AccumulatedTableUpdates accumulateTableUpdates(
      TableInfo tableInfo, List<Object> updates) {
    AccumulatedTableUpdates accumulated = new AccumulatedTableUpdates(tableInfo);
    Set<String> seenActions = new HashSet<>();

    for (Object updateObj : updates) {
      // Handle each concrete update type since they don't extend a common base class
      String action = null;

      if (updateObj instanceof AddCommitUpdate addCommit) {
        action = addCommit.getAction().getValue();
        if (addCommit.getCommit() != null) {
          accumulated.setCommit(addCommit.getCommit());
        }
      } else if (updateObj instanceof SetLatestBackfilledVersionUpdate backfillUpdate) {
        action = backfillUpdate.getAction().getValue();
        if (backfillUpdate.getLatestPublishedVersion() != null) {
          accumulated.setLatestBackfilledVersion(backfillUpdate.getLatestPublishedVersion());
        }
      } else if (updateObj instanceof SetPropertiesUpdate setProps) {
        action = setProps.getAction().getValue();
        if (setProps.getUpdates() != null && !setProps.getUpdates().isEmpty()) {
          accumulated.setProperties(setProps.getUpdates());
        }
      } else if (updateObj instanceof RemovePropertiesUpdate removeProps) {
        action = removeProps.getAction().getValue();
        if (removeProps.getRemovals() != null && !removeProps.getRemovals().isEmpty()) {
          accumulated.setRemoveProperties(removeProps.getRemovals());
        }
      } else if (updateObj instanceof SetSchemaUpdate setSchema) {
        action = setSchema.getAction().getValue();
        if (setSchema.getSchema() != null && !setSchema.getSchema().isEmpty()) {
          accumulated.setSchema(setSchema.getSchema());
        }
      } else if (updateObj instanceof SetTableCommentUpdate setComment) {
        action = setComment.getAction().getValue();
        if (setComment.getComment() != null) {
          accumulated.setComment(setComment.getComment());
        }
      } else if (updateObj instanceof UpdateProtocolUpdate protocolUpdate) {
        action = protocolUpdate.getAction().getValue();
        // Protocol updates are stored in Delta log, not in UC metadata
      } else {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown update type: " + updateObj.getClass().getName());
      }

      if (action == null) {
        throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Update action cannot be null");
      }

      // Check for duplicate actions
      if (seenActions.contains(action)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("Duplicate update action not allowed: %s", action));
      }
      seenActions.add(action);
    }

    return accumulated;
  }

  /** Container class for accumulated table updates extracted from multiple TableUpdate objects. */
  @lombok.Getter
  private static class AccumulatedTableUpdates {
    private final TableInfo tableInfo;
    private Optional<io.unitycatalog.server.model.deltarest.DeltaCommit> commit = Optional.empty();
    private Optional<Long> latestBackfilledVersion = Optional.empty();
    private Optional<Map<String, String>> setProperties = Optional.empty();
    private Optional<List<String>> removeProperties = Optional.empty();
    private Optional<List<DeltaColumn>> schema = Optional.empty();
    private Optional<String> comment = Optional.empty();

    AccumulatedTableUpdates(TableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    void setCommit(io.unitycatalog.server.model.deltarest.DeltaCommit commit) {
      this.commit = Optional.of(commit);
    }

    void setLatestBackfilledVersion(Long version) {
      this.latestBackfilledVersion = Optional.of(version);
    }

    void setProperties(Map<String, String> properties) {
      this.setProperties = Optional.of(properties);
    }

    void setRemoveProperties(List<String> properties) {
      this.removeProperties = Optional.of(properties);
    }

    void setSchema(List<DeltaColumn> schema) {
      this.schema = Optional.of(schema);
    }

    void setComment(String comment) {
      this.comment = Optional.of(comment);
    }
  }

  /**
   * Validates a table requirement for Delta REST updates.
   *
   * @param tableInfo the current table info
   * @param requirement the requirement to validate
   * @throws BaseException if the requirement is not met
   */
  private void validateTableRequirement(TableInfo tableInfo, TableRequirement requirement) {
    String type = requirement.getType();
    if (type == null) {
      return;
    }

    switch (type) {
      case "assert-table-uuid":
        UUID expectedUuid = requirement.getUuid();
        if (expectedUuid != null) {
          UUID actualUuid = UUID.fromString(tableInfo.getTableId());
          if (!expectedUuid.equals(actualUuid)) {
            throw new BaseException(
                ErrorCode.FAILED_PRECONDITION,
                String.format(
                    "Requirement failed: assert-table-uuid. Expected UUID %s but found %s",
                    expectedUuid, actualUuid));
          }
        }
        break;

      case "assert-etag":
        String expectedEtag = requirement.getEtag();
        if (expectedEtag != null) {
          String actualEtag = generateEtag(tableInfo);
          if (!expectedEtag.equals(actualEtag)) {
            throw new BaseException(
                ErrorCode.ABORTED,
                String.format(
                    "Requirement failed: assert-etag. Expected etag %s but found %s",
                    expectedEtag, actualEtag));
          }
        }
        break;

      default:
        // Unknown requirement type, skip
        break;
    }
  }

  /**
   * Applies accumulated table updates by persisting them to the database.
   *
   * @param session the Hibernate session
   * @param tableInfo the current table info
   * @param fullName the full table name
   * @param accumulated the accumulated updates to apply
   */
  private void processTableUpdates(
      Session session, TableInfo tableInfo, String fullName, AccumulatedTableUpdates accumulated) {
    DeltaCommit deltaCommit = new DeltaCommit();

    // Apply delta commit operation
    accumulated
        .getCommit()
        .ifPresent(
            commitInfo -> {
              io.unitycatalog.server.model.DeltaCommitInfo ucCommitInfo =
                  new io.unitycatalog.server.model.DeltaCommitInfo()
                      .version(commitInfo.getVersion())
                      .timestamp(commitInfo.getTimestamp())
                      .fileName(commitInfo.getFileName())
                      .fileSize(commitInfo.getFileSize())
                      .fileModificationTimestamp(commitInfo.getFileModificationTimestamp());

              deltaCommit
                  .commitInfo(ucCommitInfo);
            });

    // Apply latest backfilled version operation
    accumulated.getLatestBackfilledVersion().ifPresent(deltaCommit::setLatestBackfilledVersion);

    if (deltaCommit.getCommitInfo() != null || deltaCommit.getLatestBackfilledVersion() != null) {
      deltaCommit.tableId(tableInfo.getTableId()).tableUri(tableInfo.getStorageLocation());
      repositories.getDeltaCommitRepository().postCommit(session, deltaCommit);
    }

    // Apply table metadata updates (properties, schema, comment) in a single operation
    boolean hasMetadataChanges =
        accumulated.getSetProperties().isPresent()
            || accumulated.getRemoveProperties().isPresent()
            || accumulated.getSchema().isPresent()
            || accumulated.getComment().isPresent();

    if (hasMetadataChanges) {
      String comment = accumulated.getComment().orElse(null);

      // Compute final properties by applying set and remove operations
      Map<String, String> properties = null;
      if (accumulated.getSetProperties().isPresent()
          || accumulated.getRemoveProperties().isPresent()) {
        Map<String, String> finalProperties =
            tableInfo.getProperties() != null
                ? new HashMap<>(tableInfo.getProperties())
                : new HashMap<>();

        // Apply property additions
        accumulated.getSetProperties().ifPresent(finalProperties::putAll);

        // Apply property removals
        accumulated
            .getRemoveProperties()
            .ifPresent(toRemove -> toRemove.forEach(finalProperties::remove));

        properties = finalProperties;
      }

      List<ColumnInfo> columns =
          accumulated
              .getSchema()
              .map(TableRepository::convertDeltaColumnsToColumnInfoList)
              .orElse(null);

      updateTable(session, fullName, comment, properties, columns);
    }
  }

  /**
   * Converts Delta REST DeltaColumn list to Unity Catalog ColumnInfo list.
   *
   * @param schemaColumns the Delta REST column list
   * @return the Unity Catalog column info list
   */
  public static List<ColumnInfo> convertDeltaColumnsToColumnInfoList(
      List<DeltaColumn> schemaColumns) {
    List<ColumnInfo> columns = new ArrayList<>();
    int position = 0;
    for (DeltaColumn col : schemaColumns) {
      ColumnInfo columnInfo = new ColumnInfo();

      // Extract all information from type-json (StructField)
      StructField typeJson = col.getTypeJson();
      columnInfo.typeJson(typeJson.toString());
      columnInfo.name(typeJson.getName());
      Object typeObj = typeJson.getType();
      if (typeObj instanceof String) {
        String jsonType = (String) typeObj;
        // Map JSON type names to ColumnTypeName enum
        io.unitycatalog.server.model.ColumnTypeName typeName = switch (jsonType.toLowerCase()) {
          case "integer" -> io.unitycatalog.server.model.ColumnTypeName.INT;
          case "long" -> io.unitycatalog.server.model.ColumnTypeName.LONG;
          case "short" -> io.unitycatalog.server.model.ColumnTypeName.SHORT;
          case "byte" -> io.unitycatalog.server.model.ColumnTypeName.BYTE;
          case "float" -> io.unitycatalog.server.model.ColumnTypeName.FLOAT;
          case "double" -> io.unitycatalog.server.model.ColumnTypeName.DOUBLE;
          case "boolean" -> io.unitycatalog.server.model.ColumnTypeName.BOOLEAN;
          case "string" -> io.unitycatalog.server.model.ColumnTypeName.STRING;
          case "binary" -> io.unitycatalog.server.model.ColumnTypeName.BINARY;
          case "date" -> io.unitycatalog.server.model.ColumnTypeName.DATE;
          case "timestamp" -> io.unitycatalog.server.model.ColumnTypeName.TIMESTAMP;
          case "timestamp_ntz" -> io.unitycatalog.server.model.ColumnTypeName.TIMESTAMP_NTZ;
          default -> io.unitycatalog.server.model.ColumnTypeName.STRING;
        };
        columnInfo.typeName(typeName);
        columnInfo.typeText(jsonType);
      }
      columnInfo.nullable(typeJson.getNullable() != null ? typeJson.getNullable() : true);
      columnInfo.comment(typeJson.getComment());

      // Position is computed on-demand from array index
      columnInfo.position(position);
      position++;
      columns.add(columnInfo);
    }
    return columns;
  }

  /**
   * Generates an etag for a table based on its ID and update timestamp.
   *
   * @param tableInfo the table info
   * @return the generated etag
   */
  public String generateEtag(TableInfo tableInfo) {
    // Generate a simple etag based on table info
    String data =
        tableInfo.getTableId()
            + ":"
            + (tableInfo.getUpdatedAt() != null ? tableInfo.getUpdatedAt() : 0);
    return "\"" + Integer.toHexString(data.hashCode()) + "\"";
  }
}
