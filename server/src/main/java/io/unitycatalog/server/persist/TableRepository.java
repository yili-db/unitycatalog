package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
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
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.*;
import java.util.stream.Collectors;
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
  private static final PagedListingHelper<TableInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(TableInfoDAO.class);

  public TableRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.fileOperations = repositories.getFileOperations();
  }

  public String getStorageLocationForTableOrStagingTable(String tableId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          LOGGER.debug("Getting storage location of table by id: {}", tableId);
          UUID tableUUID = UUID.fromString(tableId);
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, tableUUID);
          if (tableInfoDAO != null) {
            return tableInfoDAO.getUrl();
          }
          LOGGER.debug("Getting storage location of staging table by id: {}", tableId);
          StagingTableDAO stagingTableDAO =
              session.get(StagingTableDAO.class, tableUUID);
          if (stagingTableDAO != null) {
            return stagingTableDAO.getStagingLocation();
          }
          throw new BaseException(ErrorCode.NOT_FOUND,
              "Neither table nor staging table found with id: " + tableId);
        },
        "Failed to get storage location of table or staging table",
        /* readOnly = */ true);
  }

  public TableInfo getTableById(String tableId) {
    LOGGER.debug("Getting table by id: {}", tableId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, UUID.fromString(tableId));
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + tableId);
          }
          SchemaInfoDAO schemaInfoDAO =
              session.get(SchemaInfoDAO.class, tableInfoDAO.getSchemaId());
          if (schemaInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Schema not found: " + tableInfoDAO.getSchemaId());
          }
          CatalogInfoDAO catalogInfoDAO =
              session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
          if (catalogInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
          }
          TableInfo tableInfo = tableInfoDAO.toTableInfo(true);
          tableInfo.setSchemaName(schemaInfoDAO.getName());
          tableInfo.setCatalogName(catalogInfoDAO.getName());
          return tableInfo;
        },
        "Failed to get table by ID",
        /* readOnly = */ true);
  }

  public TableInfo getTable(String fullName) {
    LOGGER.debug("Getting table: {}", fullName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid table name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String tableName = parts[2];
          TableInfoDAO tableInfoDAO = findTable(session, catalogName, schemaName, tableName);
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Table not found: " + fullName);
          }
          TableInfo tableInfo = tableInfoDAO.toTableInfo(true);
          tableInfo.setCatalogName(catalogName);
          tableInfo.setSchemaName(schemaName);
          RepositoryUtils.attachProperties(
              tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
          return tableInfo;
        },
        "Failed to get table",
        /* readOnly = */ true);
  }

  public String getTableUniformMetadataLocation(
      Session session, String catalogName, String schemaName, String tableName) {
    TableInfoDAO dao = findTable(session, catalogName, schemaName, tableName);
    return dao.getUniformIcebergMetadataLocation();
  }

  private TableInfoDAO findTable(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId = repositories.getSchemaRepository()
        .getSchemaId(session, catalogName, schemaName);
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
    TableInfo tableInfo =
        new TableInfo()
            .name(createTable.getName())
            .catalogName(createTable.getCatalogName())
            .schemaName(createTable.getSchemaName())
            .tableType(createTable.getTableType())
            .dataSourceFormat(createTable.getDataSourceFormat())
            .columns(columnInfos)
            .storageLocation(
                FileOperations.convertRelativePathToURI(createTable.getStorageLocation()))
            .comment(createTable.getComment())
            .properties(createTable.getProperties())
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId);
    String fullName = getTableFullName(tableInfo);
    LOGGER.debug("Creating table: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = tableInfo.getCatalogName();
          String schemaName = tableInfo.getSchemaName();
          UUID schemaId = repositories.getSchemaRepository().getSchemaId(session, catalogName,
              schemaName);

          // Check if table already exists
          TableInfoDAO existingTable =
              findBySchemaIdAndName(session, schemaId, tableInfo.getName());
          if (existingTable != null) {
            throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + fullName);
          }
          // The table ID will either be a new random one or the id of staging table, depending
          // on the type of table to create.
          String tableID;
          switch (Objects.requireNonNull(createTable.getTableType())) {
            case EXTERNAL -> {
              if (createTable.getStorageLocation() == null) {
                throw new BaseException(
                    ErrorCode.INVALID_ARGUMENT, "Storage location is required for external table");
              }
              tableID = UUID.randomUUID().toString();
            }
            case MANAGED -> {
              if (createTable.getDataSourceFormat() == DataSourceFormat.DELTA) {
                // Find and commit staging table with the same staging location
                StagingTableDAO stagingTableDAO = repositories.getStagingTableRepository()
                    .commitStagingTable(session, callerId, tableInfo.getStorageLocation());
                tableID = stagingTableDAO.getId().toString();
              } else {
                throw new BaseException(
                    ErrorCode.INVALID_ARGUMENT,
                    "MANAGED table creation is only supported for delta tables.");
              }
            }
            default -> throw new BaseException(
                ErrorCode.INVALID_ARGUMENT,
                "Unrecognized table type " + createTable.getTableType());
          }
          tableInfo.setTableId(tableID);

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

  private String getTableFullName(TableInfo tableInfo) {
    return tableInfo.getCatalogName() + "." + tableInfo.getSchemaName() + "." + tableInfo.getName();
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
          UUID schemaId = repositories.getSchemaRepository().getSchemaId(session,
              catalogName, schemaName);
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
      TableInfo tableInfo = tableInfoDAO.toTableInfo(!omitColumns);
      if (!omitProperties) {
        RepositoryUtils.attachProperties(
            tableInfo, tableInfo.getTableId(), Constants.TABLE, session);
      }
      tableInfo.setCatalogName(catalogName);
      tableInfo.setSchemaName(schemaName);
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
          UUID schemaId = repositories.getSchemaRepository().getSchemaId(session,
              catalogName, schemaName);
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
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
