package io.unitycatalog.server.service.credential;

import io.unitycatalog.server.persist.dao.CredentialDAO;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class CredentialContext {
  public enum Privilege {
    SELECT,
    UPDATE
  }

  private String storageScheme;
  private String storageBase;
  private Set<Privilege> privileges;
  // This is a list of locations to be a little future-proofing when a table could
  // have more than 1 location where files belonging to table are located
  private List<String> locations;
  // If a storage credential of external location for this path is found, the credential will
  // be generated according to it.
  private Optional<CredentialDAO> credentialDAO;

  public static CredentialContext create(URI locationURI, Set<Privilege> privileges) {
    return create(locationURI, privileges, Optional.empty());
  }

  public static CredentialContext create(
      URI locationURI, Set<Privilege> privileges, Optional<CredentialDAO> credentialDAO) {
    return CredentialContext.builder()
        .privileges(privileges)
        .storageScheme(locationURI.getScheme())
        .storageBase(generateStorageBaseFromUri(locationURI))
        .locations(List.of(locationURI.toString()))
        .credentialDAO(credentialDAO)
        .build();
  }

  public static String generateStorageBaseFromUri(URI locationURI) {
    return locationURI.getScheme() + "://" + locationURI.getAuthority();
  }
}
