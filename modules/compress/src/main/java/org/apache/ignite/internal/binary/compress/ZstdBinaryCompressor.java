package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import org.jetbrains.annotations.NotNull;

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
  public void decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    Zstd.decompressDirectByteBuffer(dest, 0, dest.limit(), src, 0, src.position());
  }

  @Override
  public int maxCompressedLength(int origSize) {
    return (int)Zstd.compressBound(origSize);
  }
}
