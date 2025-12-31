package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AzureCredentialVendor {
  private final Map<String, ADLSStorageConfig> adlsConfigurations;
  private final Map<ADLSStorageConfig, AzureCredentialsGenerator> credGenerators =
      new ConcurrentHashMap<>();

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
  }

  public AzureCredential vendAzureCredential(CredentialContext context) {
    context
        .getCredentialDAO()
        .ifPresent(
            c -> {
              throw new BaseException(
                  ErrorCode.UNIMPLEMENTED,
                  "Storage credential/external location for Azure is not supported yet.");
            });

    ADLSLocationUtils.ADLSLocationParts locParts =
        ADLSLocationUtils.parseLocation(context.getStorageBase());
    ADLSStorageConfig c = adlsConfigurations.get(locParts.accountName());
    // createAzureCredentialsGenerator still works without a config. But null can't be
    // used as key for ConcurrentHashMap. Use EMPTY instead.
    ADLSStorageConfig config = c != null ? c : ADLSStorageConfig.EMPTY;
    AzureCredentialsGenerator generator =
        credGenerators.computeIfAbsent(config, this::createAzureCredentialsGenerator);

    return generator.generate(context);
  }

  private AzureCredentialsGenerator createAzureCredentialsGenerator(ADLSStorageConfig config) {
    if (config == ADLSStorageConfig.EMPTY) {
      // EMPTY is used in place of null.
      return new AzureCredentialsGenerator.DatalakeCredentialsGenerator(null);
    }

    if (config.getCredentialsGenerator() != null) {
      try {
        return (AzureCredentialsGenerator)
            Class.forName(config.getCredentialsGenerator())
                .getDeclaredConstructor(ADLSStorageConfig.class)
                .newInstance(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (config.isTestMode()) {
      return new AzureCredentialsGenerator.StaticAzureCredentialsGenerator(config);
    } else {
      return new AzureCredentialsGenerator.DatalakeCredentialsGenerator(config);
    }
  }
}
