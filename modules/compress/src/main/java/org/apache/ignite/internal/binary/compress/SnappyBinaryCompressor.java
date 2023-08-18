package org.apache.ignite.internal.binary.compress;

import org.jetbrains.annotations.NotNull;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyBinaryCompressor implements Compressor {
  @Override
  public void compress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    Snappy.compress(src, dest);
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest, int destOff, int maxDestLen) throws IOException {
    throw new UnsupportedOperationException("Not supported, use @compress(ByteBuffer, ByteBuffer)");
  }

  @Override
  public int decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    return Snappy.uncompress(src, dest);
  }

  @Override
  public int maxCompressedLength(int origSize) {
     return Snappy.maxCompressedLength(origSize);
  }
}
