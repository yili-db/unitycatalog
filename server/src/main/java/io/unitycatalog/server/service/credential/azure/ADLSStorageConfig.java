package io.unitycatalog.server.service.credential.azure;

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
}
