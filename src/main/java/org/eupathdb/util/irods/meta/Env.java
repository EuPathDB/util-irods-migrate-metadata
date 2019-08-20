package org.eupathdb.util.irods.meta;

import java.util.Optional;

enum Env {
  IRODS_USERNAME("IRODS_USERNAME"),
  IRODS_PASSWORD("IRODS_PASSWORD"),
  IRODS_HOST("IRODS_HOST"),
  IRODS_PORT("IRODS_PORT"),
  IRODS_ZONE("IRODS_ZONE"),
  IRODS_RESOURCE("IRODS_RESOURCE");

  private static final String
    ERR_MISSING_ENV = "Missing required environment variable %s",
    ERR_INT_FORMAT  = "Environment variable %s must be an integer";

  private final String raw;

  Env(final String raw) {
    this.raw = raw;
  }

  String reqireString() {
    return Optional.ofNullable(System.getenv(raw))
      .orElseThrow(() ->
        new RuntimeException(String.format(ERR_MISSING_ENV, raw)));
  }

  int requireInt() {
    try {
      return Integer.parseInt(reqireString());
    } catch (NumberFormatException __) {
      throw new RuntimeException(String.format(ERR_INT_FORMAT, raw));
    }
  }
}
