package io.unitycatalog.server.persist.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.model.AzureServicePrincipalResponse;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.CredentialPurpose;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_credentials")
// Lombok
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class CredentialDAO extends IdentifiableDAO {
  public static ObjectMapper objectMapper = new ObjectMapper();

  public enum CredentialType {
    AWS_IAM_ROLE,
    AZURE_SERVICE_PRINCIPAL,
    // Add other types as necessary
  }

  @Column(name = "credential_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private CredentialType credentialType;

  @Lob
  @Column(name = "credential", nullable = false)
  private String credential;

  @Column(name = "purpose", nullable = false)
  private CredentialPurpose purpose;

  @Column(name = "comment")
  private String comment;

  @Column(name = "owner")
  private String owner;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Date updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  public CredentialInfo toCredentialInfo() {
    CredentialInfo credentialInfo =
        new CredentialInfo()
            .id(getId().toString())
            .name(getName())
            .purpose(getPurpose())
            .comment(getComment())
            .owner(getOwner())
            .createdAt(getCreatedAt().getTime())
            .createdBy(getCreatedBy())
            .updatedAt(getUpdatedAt() != null ? getUpdatedAt().getTime() : null)
            .updatedBy(getUpdatedBy());
    // TODO: decrypt the credential
    switch (getCredentialType()) {
      case AWS_IAM_ROLE:
        credentialInfo.setAwsIamRole(getAwsIamRoleResponse());
        break;
      case AZURE_SERVICE_PRINCIPAL:
        credentialInfo.setAzureServicePrincipal(getAzureServicePrincipalResponse());
        break;
      default:
        throw new IllegalArgumentException("Unknown credential type");
    }
    return credentialInfo;
  }

  private <T> T parseCredential(CredentialType credentialType, Class<T> clazz) {
    if (getCredentialType() != credentialType) {
      // Mismatch credential type.
      return null;
    }
    try {
      return objectMapper.readValue(getCredential(), clazz);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Failed to parse credential of " + clazz.getSimpleName(), e);
    }
  }

  // The *Response classes have no secrets and are safe to return back to clients

  public AwsIamRoleResponse getAwsIamRoleResponse() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleResponse.class);
  }

  public AzureServicePrincipalResponse getAzureServicePrincipalResponse() {
    return parseCredential(
        CredentialType.AZURE_SERVICE_PRINCIPAL, AzureServicePrincipalResponse.class);
  }

  // The *Request classes can have secrets and are meant to be used for temp cred vending only.

  public AwsIamRoleRequest getAwsIamRoleRequest() {
    return parseCredential(CredentialType.AWS_IAM_ROLE, AwsIamRoleRequest.class);
  }

  public AzureServicePrincipalRequest getAzureServicePrincipalRequest() {
    return parseCredential(
        CredentialType.AZURE_SERVICE_PRINCIPAL, AzureServicePrincipalRequest.class);
  }
}
