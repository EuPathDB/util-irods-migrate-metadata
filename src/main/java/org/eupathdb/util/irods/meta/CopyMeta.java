package org.eupathdb.util.irods.meta;

import org.irods.jargon.core.connection.ClientServerNegotiationPolicy;
import org.irods.jargon.core.connection.ClientServerNegotiationPolicy.SslNegotiationPolicy;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.IRODSGenQueryExecutor;
import org.irods.jargon.core.pub.domain.AvuData;
import org.irods.jargon.core.query.IRODSGenQuery;
import org.irods.jargon.core.query.JargonQueryException;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static java.lang.System.out;
import static org.eupathdb.util.irods.meta.Env.*;
import static org.irods.jargon.core.query.RodsGenQueryEnum.*;

public class CopyMeta {

  private static final String
    META_KEY_CONFIG = "dataset.json",
    META_KEY_META   = "meta.json";

  private static final String
    EXCLUSION_QUERY = String.format(
      "SELECT %s WHERE %s = '%s'",
      COL_COLL_NAME.getName(),
      COL_META_COLL_ATTR_NAME.getName(),
      META_KEY_CONFIG
    ),
    DATASETS_QUERY = String.format(
      "SELECT %s WHERE %s = '%s'",
      COL_COLL_NAME.getName(),
      COL_DATA_NAME.getName(),
      META_KEY_CONFIG
    );

  public static void main(final String[] args)
  throws JargonException, JargonQueryException, IOException {
    final var acc  = initAccount();
    final var sys  = IRODSFileSystem.instance();
    final var root = Paths.get("/", IRODS_ZONE.reqireString(), "workspaces/users");
    try {
      final var aof = sys.getIRODSAccessObjectFactory();
      final var idb = aof.getIRODSGenQueryExecutor(acc);
      final var fio = aof.getIRODSFileFactory(acc);
      final var cao = aof.getCollectionAO(acc);

      final var exclusions = makePathSet(idb, EXCLUSION_QUERY);
      final var datasets   = makePathSet(idb, DATASETS_QUERY);

      out.println("Copying dataset json metadata to iRODS metadata");

      for (final var dir : datasets) {

        // Unrelated entry
        if (!dir.startsWith(root))
          continue;

        // Already has metadata
        if (exclusions.contains(dir)) {
          out.println(pad("  |- " + dir) + "ALREADY DONE");
          continue;
        }

        out.println("  |- " + dir);
        final var conf = fio.instanceIRODSFile(dir.resolve(META_KEY_CONFIG).toString());
        final var meta = fio.instanceIRODSFile(dir.resolve(META_KEY_META).toString());

        if (!conf.exists())
          throw new RuntimeException("Dataset " + dir + " missing dataset.json");
        if (!meta.exists())
          throw new RuntimeException("Dataset " + dir + " missing meta.json");

        final var dsPath = dir.toString();

        out.print(pad("  |   |- " + META_KEY_CONFIG));
        cao.addAVUMetadata(dsPath, avu(META_KEY_CONFIG, stripDataFiles(
          inputToString(fio.instanceIRODSFileInputStream(conf)))));
        out.println("OK");

        out.print(pad("  |   |- " + META_KEY_META));
        cao.addAVUMetadata(dsPath, avu(META_KEY_META, inputToString(
          fio.instanceIRODSFileInputStream(meta))));
        out.println("OK");
      }


    } finally {
      sys.closeAndEatExceptions();
    }
  }

  private static String pad(final String in) {
    return in + " ".repeat(65 - in.length());
  }

  private static AvuData avu(final String key, final String value) {
    final var out = new AvuData();
    out.setAttribute(key);
    out.setValue(value);
    return out;
  }

  private static Set<Path> makePathSet(
    final IRODSGenQueryExecutor db,
    final String sql
  ) throws JargonException, JargonQueryException {
    final var query = IRODSGenQuery.instance(sql, 10_000);
    final var rs    = db.executeIRODSQueryAndCloseResult(query, 0);
    final var out   = new HashSet<Path>(rs.getTotalRecords());

    for (final var row : rs.getResults())
      out.add(Paths.get(row.getColumn(COL_COLL_NAME.getName())));

    return out;
  }

  private static IRODSAccount initAccount() throws JargonException {
    final var user = IRODS_USERNAME.reqireString();
    final var zone = IRODS_ZONE.reqireString();
    final var csnp = new ClientServerNegotiationPolicy();

    csnp.setSslNegotiationPolicy(SslNegotiationPolicy.CS_NEG_REQUIRE);

    return IRODSAccount.instance(
      IRODS_HOST.reqireString(),
      IRODS_PORT.requireInt(),
      user,
      IRODS_PASSWORD.reqireString(),
      String.format("/%s/home/%s", zone, user),
      zone,
      IRODS_RESOURCE.reqireString(),
      csnp
    );
  }

  private static String stripDataFiles(final String input) {
    final var json = new JSONObject(input);
    json.remove("dataFiles");
    return json.toString();
  }

  private static String inputToString(final InputStream input) throws IOException {
    try (
      final var in  = new InputStreamReader(input);
      final var out = new StringWriter()
    ) {
      Pipe.between(in, out);
      return out.toString();
    }
  }
}
