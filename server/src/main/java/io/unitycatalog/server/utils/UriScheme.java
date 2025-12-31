package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.HashMap;
import java.util.Map;

public enum UriScheme {
  ABFS(Constants.URI_SCHEME_ABFS),
  ABFSS(Constants.URI_SCHEME_ABFSS),
  GS(Constants.URI_SCHEME_GS),
  S3(Constants.URI_SCHEME_S3),
  FILE(Constants.URI_SCHEME_FILE),
  NULL(null);

  public final String scheme;
  private static final Map<String, UriScheme> SCHEME_MAP = new HashMap<>();

  // Populate the cache statically
  static {
    for (UriScheme s : values()) {
      SCHEME_MAP.put(s.scheme, s);
    }
    assert SCHEME_MAP.keySet().containsAll(Constants.SUPPORTED_CLOUD_SCHEMES);
  }

  UriScheme(String scheme) {
    this.scheme = scheme;
  }

  public static UriScheme fromString(String scheme) {
    // null is also converted into NULL
    UriScheme res = SCHEME_MAP.get(scheme);
    if (res == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + scheme);
    }
    return res;
  }
}
