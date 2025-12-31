package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder(toBuilder = true)
@ToString
public class S3StorageConfig {
  private final String bucketPath;
  private final String region;
  private final String awsRoleArn;
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;
  private final String credentialsGenerator;

  public S3StorageConfig mergeCredentialDAO(CredentialDAO credentialDAO) {
    AwsIamRoleResponse awsIamRole = credentialDAO.getAwsIamRoleResponse();
    if (awsIamRole == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Storage credential '" + credentialDAO.getName() + "' does not contain AwsIamRole");
    }
    if (awsIamRole.getRoleArn() == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Storage credential '" + credentialDAO.getName() + "' does not contain an AWS role ARN");
    }
    return toBuilder().awsRoleArn(awsIamRole.getRoleArn()).build();
  }
}
