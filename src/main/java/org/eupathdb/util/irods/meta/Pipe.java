package org.eupathdb.util.irods.meta;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

final class Pipe {
  private static final int BUF_SIZE = 16_384;
  private static final char[] BUFFER = new char[BUF_SIZE];

  static void between(final Reader in, final Writer out) throws IOException {
    int n;
    while ((n = in.read(BUFFER)) > -1)
      out.write(BUFFER, 0, n);
    out.flush();
  }
}
