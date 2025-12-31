package io.unitycatalog.server.sdk.tempcredential;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.sdk.storagecredential.SdkCredentialOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.CredentialsGenerator;
import io.unitycatalog.server.utils.TestUtils;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.model.Credentials;

@Slf4j
public class TemporaryPathCredentialsServiceTest extends SdkAccessControlBaseCRUDTest {

  private static final String TEST_EXTERNAL_LOCATION_NAME = "test_ext_loc";
  private static final String TEST_EXTERNAL_LOCATION_URL = "s3://test-bucket0/path/to/data";
  private static final String TEST_CREDENTIAL_NAME = "test_credential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String CREDENTIALS_GENERATOR_CLASS =
    TestAwsCredentialsGenerator.class.getName();

  private SdkCredentialOperations adminCredentialOperations;
  private TemporaryCredentialsApi adminTempCredsApi;

  private ExternalLocationsApi locationOwnerExternalLocationsApi;
  private TemporaryCredentialsApi locationOwnerTempCredsApi;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // Configure S3 credentials with custom generator
    // Note: access/secret keys are required even when using a credentials generator
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");
    serverProperties.put("s3.credentialsGenerator.0", CREDENTIALS_GENERATOR_CLASS);
  }

  @SneakyThrows
  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    adminCredentialOperations = new SdkCredentialOperations(TestUtils.createApiClient(adminConfig));
    adminTempCredsApi = new TemporaryCredentialsApi(TestUtils.createApiClient(adminConfig));

    // Create a credential
    CreateCredentialRequest createCredentialRequest =
      new CreateCredentialRequest()
        .name(TEST_CREDENTIAL_NAME)
        .comment("Test credential for temporary path credentials")
        .purpose(CredentialPurpose.STORAGE)
        .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    CredentialInfo credentialInfo =
      adminCredentialOperations.createCredential(createCredentialRequest);
    assertThat(credentialInfo).isNotNull();

    // Create a user and grant it permission to create a location
    String locationOwnerEmail = "location_owner@example.com";
    ApiClient locationOwnerClientApi = createApiClientForNewUser(locationOwnerEmail, List.of());
    locationOwnerExternalLocationsApi = new ExternalLocationsApi(locationOwnerClientApi);
    locationOwnerTempCredsApi = new TemporaryCredentialsApi(locationOwnerClientApi);
    grantPermissions(
        locationOwnerEmail,
        SecurableType.METASTORE,
        METASTORE_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);
    grantPermissions(
        locationOwnerEmail,
        SecurableType.CREDENTIAL,
        TEST_CREDENTIAL_NAME,
        Privileges.CREATE_EXTERNAL_LOCATION);

    // Create external location as locationOwner (not admin)
    CreateExternalLocation createExternalLocation =
      new CreateExternalLocation()
        .name(TEST_EXTERNAL_LOCATION_NAME)
        .url(TEST_EXTERNAL_LOCATION_URL)
        .credentialName(TEST_CREDENTIAL_NAME)
        .comment("Test external location");
    ExternalLocationInfo externalLocationInfo =
      locationOwnerExternalLocationsApi.createExternalLocation(createExternalLocation);
    assertThat(externalLocationInfo).isNotNull();
  }

  @AfterEach
  @Override
  public void tearDown() {
    // Cleanup: delete the external location (as locationOwner who created it)
    try {
      locationOwnerExternalLocationsApi.deleteExternalLocation(TEST_EXTERNAL_LOCATION_NAME, true);
    } catch (ApiException e) {
      // Pass
    }
    try {
      adminCredentialOperations.deleteCredential(TEST_CREDENTIAL_NAME);
    } catch (ApiException e) {
      // Pass
    }
    super.tearDown();
  }

  @SneakyThrows
  private ApiClient createApiClientForNewUser(String email,
    List<Privileges> privileges) {
    createTestUser(email, email.split("@")[0]);
    for (Privileges privilege : privileges) {
      grantPermissions(email, SecurableType.EXTERNAL_LOCATION, TEST_EXTERNAL_LOCATION_NAME,
        privilege);
    }
    ServerConfig config = createTestUserServerConfig(email);
    return TestUtils.createApiClient(config);
  }

  private TemporaryCredentialsApi createTempCredApiForNewUser(String email,
    List<Privileges> privileges) {
    return new TemporaryCredentialsApi(createApiClientForNewUser(email, privileges));
  }

  private record TemporaryPathCredentialsTestCase(
    TemporaryCredentialsApi api,
    boolean canRead,
    boolean canWrite,
    boolean canCreateExternalTable
  ) {
    boolean canReadWrite() {
      return canRead && canWrite;
    }
  }

  @Test
  public void testTemporaryPathCredentialsAuthorization() throws Exception {
    // Create test users and their API clients.
    // Test different authorization scenarios:
    // - adminTempCredsApi: metastore owner (admin) but NOT external location owner
    // - locationOwnerTempCredsApi: external location owner but NOT metastore owner
    // - readOnlyUser: has READ_FILES privilege - can do PATH_READ only
    // - readWriteUser: has READ_FILES and WRITE_FILES privileges - can do PATH_READ and
    // PATH_READ_WRITE
    // - createTableUser: has CREATE_EXTERNAL_TABLE privilege - can do PATH_CREATE_TABLE only
    // - Unauthorized user: no privileges on the external location
    TemporaryCredentialsApi readOnlyTempCredsApi =
      createTempCredApiForNewUser("readonly@example.com",
        List.of(Privileges.READ_FILES));
    TemporaryCredentialsApi readWriteTempCredsApi =
      createTempCredApiForNewUser("readwrite@example.com",
        List.of(Privileges.READ_FILES, Privileges.WRITE_FILES));
    TemporaryCredentialsApi createTableTempCredsApi =
      createTempCredApiForNewUser("createtable@example.com",
        List.of(Privileges.CREATE_EXTERNAL_TABLE));
    TemporaryCredentialsApi unauthorizedTempCredsApi =
      createTempCredApiForNewUser("unauthorized@example.com", List.of());

    // For URLs under the external location, follow external location permission.
    List<String> matchingUrls =
      List.of(TEST_EXTERNAL_LOCATION_URL, TEST_EXTERNAL_LOCATION_URL + "/subdir/nested");
    List<TemporaryPathCredentialsTestCase> testCasesWithMatchingUrls = List.of(
      new TemporaryPathCredentialsTestCase(adminTempCredsApi, true, true, true),
      new TemporaryPathCredentialsTestCase(locationOwnerTempCredsApi, true, true, true),
      new TemporaryPathCredentialsTestCase(readWriteTempCredsApi, true, true, false),
      new TemporaryPathCredentialsTestCase(readOnlyTempCredsApi, true, false, false),
      new TemporaryPathCredentialsTestCase(createTableTempCredsApi, false, false, true),
      new TemporaryPathCredentialsTestCase(unauthorizedTempCredsApi, false, false, false)
    );
    testPathCredentials(matchingUrls, testCasesWithMatchingUrls);

    // For URLs outside the external location, only metastore owner can get credential
    List<String> nonMatchingUrls =
      List.of("s3://test-bucket0/different/path");
    List<TemporaryPathCredentialsTestCase> testCasesWithNonMatchingUrls = List.of(
      new TemporaryPathCredentialsTestCase(adminTempCredsApi, true, true, true),
      new TemporaryPathCredentialsTestCase(locationOwnerTempCredsApi, false, false, false),
      new TemporaryPathCredentialsTestCase(unauthorizedTempCredsApi, false, false, false)
    );
    testPathCredentials(nonMatchingUrls, testCasesWithNonMatchingUrls);
  }

  private void testPathCredentials(List<String> urls,
    List<TemporaryPathCredentialsTestCase> testCases) {
    Map<PathOperation, Function<TemporaryPathCredentialsTestCase, Boolean>> subTestCases =
      Map.of(PathOperation.PATH_READ, TemporaryPathCredentialsTestCase::canRead,
        PathOperation.PATH_READ_WRITE, TemporaryPathCredentialsTestCase::canReadWrite,
        PathOperation.PATH_CREATE_TABLE, TemporaryPathCredentialsTestCase::canCreateExternalTable);

    for (String url : urls) {
      for (TemporaryPathCredentialsTestCase testCase : testCases) {
        subTestCases.forEach((operation, getExpectSuccess) -> {
          boolean expectSuccess = getExpectSuccess.apply(testCase);
          if (expectSuccess) {
            testPathCredentialsSuccess(testCase.api, url, operation);
          } else {
            testPermissionDenied(testCase.api, url, operation);
          }
        });
      }
    }
  }

  @SneakyThrows
  private void testPathCredentialsSuccess(
    TemporaryCredentialsApi api, String url, PathOperation operation) {
    GenerateTemporaryPathCredential request =
      new GenerateTemporaryPathCredential().url(url).operation(operation);
    TemporaryCredentials creds = api.generateTemporaryPathCredentials(request);
    assertThat(creds).isNotNull();
    assertValidTemporaryCredentials(creds);
  }

  private void testPermissionDenied(
    TemporaryCredentialsApi api, String url, PathOperation operation) {
    GenerateTemporaryPathCredential request =
      new GenerateTemporaryPathCredential().url(url).operation(operation);
    assertApiException(
      () -> api.generateTemporaryPathCredentials(request),
      ErrorCode.PERMISSION_DENIED,
      "PERMISSION_DENIED");
  }

  private void assertValidTemporaryCredentials(TemporaryCredentials credentials) {
    assertThat(credentials).isNotNull();
    log.error("yili credentials={}", credentials);
    assertThat(credentials.getAwsTempCredentials()).isNotNull();

    AwsCredentials awsCreds = credentials.getAwsTempCredentials();
    assertThat(awsCreds.getAccessKeyId()).isNotNull().isNotEmpty();
    assertThat(awsCreds.getSecretAccessKey()).isNotNull().isNotEmpty();
    assertThat(awsCreds.getSessionToken()).isNotNull().isNotEmpty();

    // Expiration time should be set
    assertThat(credentials.getExpirationTime()).isNotNull();
  }

  public static class TestAwsCredentialsGenerator implements CredentialsGenerator {
    @Override
    public Credentials generate(CredentialContext ctx) {
      return Credentials.builder()
        .accessKeyId("test-access-key-id")
        .secretAccessKey("test-secret-access-key")
        .sessionToken("test-session-token")
        .expiration(Instant.now().plusSeconds(3600)).build();
    }
  }
}
