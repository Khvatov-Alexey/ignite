package org.apache.ignite.internal.binary.compress;

import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyBinaryCompressor implements Compressor {

  private final ThreadLocalDirectByteBuffer tlDirectBuf = new ThreadLocalDirectByteBuffer();

  @Override
  public void compress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    Snappy.compress(src, dest);
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest, int destOff, int maxDestLen) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    assert destOff < dest.capacity() : "Destination offset more than buffer capacity";
    assert maxDestLen + destOff < dest.capacity() : "Destination offset and length more than buffer capacity";

    ByteBuffer buf = tlDirectBuf.get(maxDestLen);
    Snappy.compress(src, buf);

    byte[] array = new byte[buf.remaining()];
    buf.get(array);

    dest.put(array, destOff, array.length);
    return array.length;
  }

  @Override
  public void decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    Snappy.uncompress(src, dest);
  }

  @Override
  public int maxCompressedLength(int origSize) {
     return Snappy.maxCompressedLength(origSize);
  }
}
