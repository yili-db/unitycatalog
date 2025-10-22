package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingTableRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(StagingTableRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;

  public StagingTableRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public StagingTableInfo getStagingTableById(String stagingTableId) {
    LOGGER.debug("Getting staging table by id: {}", stagingTableId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          StagingTableDAO stagingTableDAO =
              session.get(StagingTableDAO.class, UUID.fromString(stagingTableId));
          if (stagingTableDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Staging table not found: " + stagingTableId);
          }
          SchemaInfoDAO schemaInfoDAO =
              session.get(SchemaInfoDAO.class, stagingTableDAO.getSchemaId());
          if (schemaInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Schema not found: " + stagingTableDAO.getSchemaId());
          }
          CatalogInfoDAO catalogInfoDAO =
              session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
          if (catalogInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
          }
          return stagingTableDAO.toStagingTableInfo(
              catalogInfoDAO.getName(), schemaInfoDAO.getName());
        },
        "Failed to get staging table by ID",
        /* readOnly = */ true);
  }

  private StagingTableDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql = "FROM StagingTableDAO t WHERE t.schemaId = :schemaId AND t.name = :name";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding staging table by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  public StagingTableDAO findByStagingLocation(Session session, String stagingLocation) {
    String hql = "FROM StagingTableDAO t WHERE t.stagingLocation = :stagingLocation";
    Query<StagingTableDAO> query = session.createQuery(hql, StagingTableDAO.class);
    query.setParameter("stagingLocation", stagingLocation);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private void validateIfAlreadyExists(
      Session session, UUID schemaId, String tableName, String stagingLocation) {
    // check if staging table or table by the same name already exists
    // Also ensure that no staging table exists at the same location
    StagingTableDAO existingStagingTable = findBySchemaIdAndName(session, schemaId, tableName);
    if (existingStagingTable != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists: " + tableName);
    }
    TableInfoDAO existingTable =
        repositories.getTableRepository().findBySchemaIdAndName(session, schemaId, tableName);
    if (existingTable != null) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Table already exists: " + tableName);
    }
    StagingTableDAO existingStagingTableAtLocation =
        findByStagingLocation(session, stagingLocation);
    if (existingStagingTableAtLocation != null) {
      throw new BaseException(
          ErrorCode.ALREADY_EXISTS, "Staging table already exists: " + stagingLocation);
    }
  }

  private String getTableFullName(CreateStagingTable createStagingTable) {
    return createStagingTable.getCatalogName()
        + "."
        + createStagingTable.getSchemaName()
        + "."
        + createStagingTable.getName();
  }

  public StagingTableInfo createStagingTable(CreateStagingTable createStagingTable) {
    ValidationUtils.validateSqlObjectName(createStagingTable.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID stagingTableId = UUID.randomUUID();
    String stagingLocation =
        repositories.getFileOperations().createTableDirectory(stagingTableId.toString());

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaId(
                      session,
                      createStagingTable.getCatalogName(),
                      createStagingTable.getSchemaName());
          validateIfAlreadyExists(session, schemaId, createStagingTable.getName(), stagingLocation);

          StagingTableDAO stagingTableDAO = new StagingTableDAO();
          stagingTableDAO.setDefaultFields();
          stagingTableDAO.setId(stagingTableId);
          stagingTableDAO.setSchemaId(schemaId);
          stagingTableDAO.setName(createStagingTable.getName());
          stagingTableDAO.setStagingLocation(stagingLocation);
          stagingTableDAO.setCreatedBy(callerId);
          session.persist(stagingTableDAO);
          return stagingTableDAO.toStagingTableInfo(
              createStagingTable.getCatalogName(), createStagingTable.getSchemaName());
        },
        "Error creating table: " + getTableFullName(createStagingTable),
        /* readOnly = */ false);
  }

  /**
   * This function is called by TableRepository.createTable when the table is created on top of a
   * staging table location.
   */
  public StagingTableDAO commitStagingTable(
      Session session, String callerId, String storageLocation) {
    StagingTableDAO stagingTableDAO = findByStagingLocation(session, storageLocation);
    if (stagingTableDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Staging table not found: " + storageLocation);
    }
    // Commit the staging table
    if (stagingTableDAO.isStageCommitted()) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "Staging table already committed: " + storageLocation);
    }
    if (!Objects.equals(stagingTableDAO.getCreatedBy(), callerId)) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED,
          "User attempts to create table on a staging location without ownership: "
              + storageLocation);
    }

    Date now = new Date();
    stagingTableDAO.setStageCommitted(true);
    stagingTableDAO.setStageCommittedAt(now);
    stagingTableDAO.setAccessedAt(now);
    session.persist(stagingTableDAO);
    return stagingTableDAO;
  }
}
