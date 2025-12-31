package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

public class FileIOFactory {

  private final StorageCredentialVendor storageCredentialVendor;
  private final Map<String, S3StorageConfig> s3Configurations;

  public FileIOFactory(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.s3Configurations = serverProperties.getS3Configurations();
  }

  // TODO: Cache fileIOs
  public FileIO getFileIO(String path) {
    Map<String, String> config = getFileIOConfig(path);
    if (config.isEmpty()) {
      // TODO: should we default/fallback to HadoopFileIO? Currently there would be some dependency
      //  issue when depending on hadoop-client-runtime
      return new SimpleLocalFileIO();
    }
    ResolvingFileIO fileio = new ResolvingFileIO();
    fileio.initialize(config);
    return fileio;
  }

  public Map<String, String> getFileIOConfig(String path) {
    // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't
    //  support write
    TemporaryCredentials cred =
        storageCredentialVendor.vendCredential(path, Set.of(CredentialContext.Privilege.SELECT));
    if (cred.getAzureUserDelegationSas() != null) {
      return getADLSConfig(path, cred.getAzureUserDelegationSas());
    } else if (cred.getGcpOauthToken() != null) {
      return getGCSConfig(cred.getGcpOauthToken(), cred.getExpirationTime());
    } else if (cred.getAwsTempCredentials() != null) {
      return getS3Config(path, cred.getAwsTempCredentials());
    } else {
      // TODO: should we default/fallback to HadoopFileIO ?
      return Map.of();
    }
  }

  protected Map<String, String> getADLSConfig(
      String path, AzureUserDelegationSAS azureUserDelegationSAS) {
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(path);
    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    return Map.of(
        AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(),
        azureUserDelegationSAS.getSasToken());
  }

  protected Map<String, String> getGCSConfig(GcpOauthToken gcpOauthToken, Long expirationTime) {
    if (expirationTime != null) {
      return Map.of(
          GCPProperties.GCS_OAUTH2_TOKEN,
          gcpOauthToken.getOauthToken(),
          GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
          Long.toString(expirationTime));
    } else {
      return Map.of(GCPProperties.GCS_OAUTH2_TOKEN, gcpOauthToken.getOauthToken());
    }
  }

  protected Map<String, String> getS3Config(String path, AwsCredentials awsCredentials) {
    URI uri = URI.create(path);
    String storageBase = CredentialContext.generateStorageBaseFromUri(uri);
    S3StorageConfig s3StorageConfig = s3Configurations.get(storageBase);
    return Map.of(
        S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.getAccessKeyId(),
        S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.getSecretAccessKey(),
        S3FileIOProperties.SESSION_TOKEN, awsCredentials.getSessionToken(),
        AwsClientProperties.CLIENT_REGION, s3StorageConfig.getRegion());
  }
}
