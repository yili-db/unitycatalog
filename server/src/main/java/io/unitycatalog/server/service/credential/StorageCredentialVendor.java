package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PathBasedRpcUtils;
import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class StorageCredentialVendor {

  CloudCredentialVendor cloudCredentialVendor;
  private final PathBasedRpcUtils pathBasedRpcUtils;

  public StorageCredentialVendor(
      CloudCredentialVendor cloudCredentialVendor, PathBasedRpcUtils pathBasedRpcUtils) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.pathBasedRpcUtils = pathBasedRpcUtils;
  }

  public TemporaryCredentials vendCredential(
      String path, Set<CredentialContext.Privilege> privileges) {
    // Permission authorization is already done before calling this function.
    if (path == null || path.isEmpty()) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Storage location is null or empty.");
    }
    String standardizedUrl = FileOperations.toStandardizedURIString(path);
    Optional<CredentialDAO> credentialDAO =
        pathBasedRpcUtils.getExternalLocationCredentialForPath(standardizedUrl);

    URI storageLocationUri = URI.create(standardizedUrl);
    CredentialContext credentialContext =
        CredentialContext.create(storageLocationUri, privileges, credentialDAO);
    return cloudCredentialVendor.vendCredential(credentialContext);
  }
}
