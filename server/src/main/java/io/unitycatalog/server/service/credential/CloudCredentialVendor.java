package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import software.amazon.awssdk.services.sts.model.Credentials;

public class CloudCredentialVendor {

  private final AwsCredentialVendor awsCredentialVendor;
  private final AzureCredentialVendor azureCredentialVendor;
  private final GcpCredentialVendor gcpCredentialVendor;

  public CloudCredentialVendor(AwsCredentialVendor awsCredentialVendor,
    AzureCredentialVendor azureCredentialVendor, GcpCredentialVendor gcpCredentialVendor) {
    this.awsCredentialVendor = awsCredentialVendor;
    this.azureCredentialVendor = azureCredentialVendor;
    this.gcpCredentialVendor = gcpCredentialVendor;
  }

  public CloudCredentialVendor(ServerProperties serverProperties) {
    this.awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    this.azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    this.gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
  }

  public TemporaryCredentials vendCredential(CredentialContext context) {
    String location = context.getLocations().get(0);
    FileOperations.assertValidLocation(location);

    return switch (UriScheme.fromString(context.getStorageScheme())) {
      case ABFS, ABFSS -> vendAzureCredential(context);
      case GS -> vendGcpToken(context);
      case S3 -> vendAwsCredential(context);
      // For local file system, we return empty credentials
      case FILE, NULL -> new TemporaryCredentials();
    };
  }

  public TemporaryCredentials vendAwsCredential(CredentialContext context) {
    TemporaryCredentials temporaryCredentials = new TemporaryCredentials();
    Credentials awsSessionCredentials = awsCredentialVendor.vendAwsCredentials(context);
    temporaryCredentials.awsTempCredentials(
        new AwsCredentials()
            .accessKeyId(awsSessionCredentials.accessKeyId())
            .secretAccessKey(awsSessionCredentials.secretAccessKey())
            .sessionToken(awsSessionCredentials.sessionToken()));

    // Explicitly set the expiration time for the temporary credentials if it's a non-static
    // credential. For static credential, the expiration time can be nullable.
    if (awsSessionCredentials.expiration() != null) {
      temporaryCredentials.expirationTime(awsSessionCredentials.expiration().toEpochMilli());
    }
    return temporaryCredentials;
  }

  public TemporaryCredentials vendAzureCredential(CredentialContext context) {
    AzureCredential azureCredential = azureCredentialVendor.vendAzureCredential(context);
    return new TemporaryCredentials()
        .azureUserDelegationSas(
            new AzureUserDelegationSAS().sasToken(azureCredential.getSasToken()))
        .expirationTime(azureCredential.getExpirationTimeInEpochMillis());
  }

  public TemporaryCredentials vendGcpToken(CredentialContext context) {
    AccessToken gcpToken = gcpCredentialVendor.vendGcpToken(context);
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
        .expirationTime(gcpToken.getExpirationTime().getTime());
  }
}
