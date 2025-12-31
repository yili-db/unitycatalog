package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class ADLSStorageConfig {
  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final String credentialsGenerator;
  private final boolean testMode;

  public static final ADLSStorageConfig EMPTY = ADLSStorageConfig.builder().build();

  public static ADLSStorageConfig fromCredentialDAO(CredentialDAO credentialDAO) {
    AzureServicePrincipalRequest azureServicePrincipal =
        credentialDAO.getAzureServicePrincipalRequest();
    if (azureServicePrincipal == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Storage credential '"
              + credentialDAO.getName()
              + "' does not contain AzureServicePrincipal");
    }
    return ADLSStorageConfig.builder()
        .tenantId(azureServicePrincipal.getDirectoryId())
        .clientId(azureServicePrincipal.getApplicationId())
        .clientSecret(azureServicePrincipal.getClientSecret())
        .testMode(false)
        .build();
  }
}
