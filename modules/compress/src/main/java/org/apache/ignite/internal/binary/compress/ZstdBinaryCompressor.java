package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ZstdBinaryCompressor implements Compressor {
  private final int level;

  public ZstdBinaryCompressor(int level) {
    this.level = level;
  }

  @Override
  public void compress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    Zstd.compressDirectByteBuffer(dest, 0, dest.limit(), src, 0, src.position(), level);
  }

  @Override
  public int compress(ByteBuffer src, ByteBuffer dest, int destOff, int maxDestLen) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";
    assert destOff < dest.capacity() : "Destination offset more than buffer capacity";
    assert maxDestLen + destOff <= dest.capacity() : "Destination offset and length more than buffer capacity";

    long size = Zstd.compressDirectByteBuffer(dest, destOff, maxDestLen, src, src.position(), src.remaining(), level);
    return size <= Integer.MAX_VALUE ? (int) size : -1;
  }

  @Override
  public int decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    return (int) Zstd.decompressDirectByteBuffer(dest, 0, dest.limit(), src, src.position(), src.remaining());
  }

  @Override
  public int maxCompressedLength(int origSize) {
    long maxDstSize = Zstd.compressBound(origSize);
    return maxDstSize <= Integer.MAX_VALUE ? (int) maxDstSize : -1;
  }
}
