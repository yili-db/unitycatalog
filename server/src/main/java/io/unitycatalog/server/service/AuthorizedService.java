package io.unitycatalog.server.service;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;

import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;

import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;

/**
 * Abstract service class that provides common authorization functionality for all Unity Catalog
 * services.
 */
public abstract class AuthorizedService {
  protected final UnityCatalogAuthorizer authorizer;
  protected final UnityAccessEvaluator evaluator;
  protected final UserRepository userRepository;
  protected final KeyMapper keyMapper;

  @SneakyThrows
  protected AuthorizedService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.userRepository = repositories.getUserRepository();
    this.keyMapper = repositories.getKeyMapper();
  }

  /**
   * Initializes basic authorization for a resource by granting owner privileges to the current
   * principal.
   *
   * @param resourceId String ID of the resource to grant permission for
   */
  protected void initializeBasicAuthorization(String resourceId) {
    UUID principalId = userRepository.findPrincipalId();
    authorizer.grantAuthorization(principalId, UUID.fromString(resourceId), Privileges.OWNER);
  }

  /**
   * Initializes hierarchical authorization for a resource by granting owner privileges to the
   * current principal and establishing a parent-child relationship.
   *
   * @param resourceId String ID of the resource to grant permission for
   * @param parentId String ID of the parent resource
   */
  protected void initializeHierarchicalAuthorization(String resourceId, String parentId) {
    initializeBasicAuthorization(resourceId);
    authorizer.addHierarchyChild(UUID.fromString(parentId), UUID.fromString(resourceId));
  }

  /**
   * Removes all authorizations for a resource.
   *
   * @param resourceId String ID of the resource to remove authorizations for
   */
  protected void removeAuthorizations(String resourceId) {
    authorizer.clearAuthorizationsForResource(UUID.fromString(resourceId));
  }

  /**
   * Removes all authorizations for a resource and removes the parent-child relationship.
   *
   * @param resourceId String ID of the resource to remove authorizations for
   * @param parentId String ID of the parent resource
   */
  protected void removeHierarchicalAuthorizations(String resourceId, String parentId) {
    removeAuthorizations(resourceId);
    authorizer.removeHierarchyChild(UUID.fromString(parentId), UUID.fromString(resourceId));
  }

  protected void authorizeExternalLocationUse(SecurableType securableType, String storageLocation) {
    // Additional authorization needed for external location IF the storage location is located
    // under an existing external location.
    String privilege = switch(securableType) {
      case TABLE -> "CREATE_EXTERNAL_TABLE";
      case VOLUME -> "CREATE_EXTERNAL_VOLUME";
      default -> throw new BaseException(ErrorCode.INTERNAL,
        "Unsupported securable type to use external location: " + securableType);
    };
    // Creating a new external entity outside all external location is allowed.
    // Creating a new external entity inside an external location needs OWNER of metastore or
    //  external_location, or 'privilege' of the external location.
    // In both cases, the path should not belong to an existing data entity (table, volume, etc).
    String authorizeExpression = String.format("""
      (#external_location == null ||
       #authorize(#principal, #metastore, OWNER) ||
       #authorizeAny(#principal, #external_location, OWNER, %s)) &&
      #table == null &&
      #volume == null
      """, privilege);
    Map<SecurableType, Object> resourceKeys =
      keyMapper.mapResourceKeys(
        Map.of(METASTORE, "metastore", EXTERNAL_LOCATION, storageLocation));
    if (!evaluator.evaluate(userRepository.findPrincipalId(), authorizeExpression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }
}
