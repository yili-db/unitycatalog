package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ListCredentialsResponse;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialCli {

  private static final Logger log = LoggerFactory.getLogger(CredentialCli.class);
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    CredentialsApi credentialsApi = new CredentialsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createCredential(credentialsApi, json);
        break;
      case CliUtils.LIST:
        output = listCredentials(credentialsApi, json);
        break;
      case CliUtils.GET:
        output = getCredential(credentialsApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateCredential(credentialsApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteCredential(credentialsApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.CREDENTIAL);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static Optional<AwsIamRoleRequest> extractAwsIamRole(JSONObject json) {
    if (json.has(CliParams.AWS_IAM_ROLE_ARN.getServerParam())) {
      String awsIamRoleArnStr = json.getString(CliParams.AWS_IAM_ROLE_ARN.getServerParam());
      return Optional.of(new AwsIamRoleRequest().roleArn(awsIamRoleArnStr));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<AzureServicePrincipalRequest> extractAzureServicePrincipal(
      JSONObject json) {
    final Map<CliParams, BiConsumer<AzureServicePrincipalRequest, String>> azureParamMapping =
        Map.of(
            CliParams.AZURE_DIRECTORY_ID, AzureServicePrincipalRequest::setDirectoryId,
            CliParams.AZURE_APPLICATION_ID, AzureServicePrincipalRequest::setApplicationId,
            CliParams.AZURE_CLIENT_SECRET, AzureServicePrincipalRequest::setClientSecret);
    final List<String> azureParams =
        azureParamMapping.keySet().stream().map(CliParams::getServerParam).toList();
    // All or nothing
    if (azureParams.stream().anyMatch(json::has)) {
      if (!json.keySet().containsAll(azureParams)) {
        throw new CliException(
            "To create credential for Azure service principal, all of directory "
                + "id, application id and client secret are requried.");
      }
      AzureServicePrincipalRequest azureServicePrincipal = new AzureServicePrincipalRequest();
      // Set the corresponding fields in AzureServicePrincipalRequest using values out of json.
      azureParamMapping.forEach(
          (param, setter) ->
              setter.accept(azureServicePrincipal, json.getString(param.getServerParam())));
      return Optional.of(azureServicePrincipal);
    } else {
      return Optional.empty();
    }
  }

  private static String createCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    log.error("yili create json={}", json);
    CreateCredentialRequest createCredentialRequest =
        new CreateCredentialRequest()
            .name(json.getString(CliParams.NAME.getServerParam()))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(extractAwsIamRole(json).orElse(null))
            .azureServicePrincipal(extractAzureServicePrincipal(json).orElse(null));
    log.error("yili create createCredentialRequest={}", createCredentialRequest);
    CredentialInfo credentialInfo = credentialsApi.createCredential(createCredentialRequest);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String listCredentials(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    int maxResults = json.optInt(CliParams.MAX_RESULTS.getServerParam(), 100);
    String pageToken = json.optString(CliParams.PAGE_TOKEN.getServerParam(), null);
    ListCredentialsResponse listCredentialsResponse =
        credentialsApi.listCredentials(maxResults, pageToken, null);
    List<CredentialInfo> credentials = listCredentialsResponse.getCredentials();
    return objectWriter.writeValueAsString(credentials);
  }

  private static String getCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    CredentialInfo credentialInfo = credentialsApi.getCredential(name);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String updateCredential(CredentialsApi credentialsApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    UpdateCredentialRequest updateCredentialRequest =
        new UpdateCredentialRequest()
            .newName(json.optString(CliParams.NEW_NAME.getServerParam(), null))
            .comment(json.optString(CliParams.COMMENT.getServerParam(), null))
            .awsIamRole(extractAwsIamRole(json).orElse(null))
            .azureServicePrincipal(extractAzureServicePrincipal(json).orElse(null));
    CredentialInfo credentialInfo = credentialsApi.updateCredential(name, updateCredentialRequest);
    return objectWriter.writeValueAsString(credentialInfo);
  }

  private static String deleteCredential(CredentialsApi credentialsApi, JSONObject json)
      throws ApiException {
    String name = json.getString(CliParams.NAME.getServerParam());
    boolean force = json.optBoolean(CliParams.FORCE.getServerParam(), false);
    credentialsApi.deleteCredential(name, force);
    return CliUtils.EMPTY_JSON;
  }
}
