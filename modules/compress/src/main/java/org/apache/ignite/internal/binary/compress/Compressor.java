package org.apache.ignite.internal.binary.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Compressor {
  void compress(ByteBuffer src, ByteBuffer dest) throws IOException;

  void decompress(ByteBuffer src, ByteBuffer dest) throws IOException;

  int maxCompressedLength(int origSize);
}
