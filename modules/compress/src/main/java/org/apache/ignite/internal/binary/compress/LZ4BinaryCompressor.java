package org.apache.ignite.internal.binary.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class LZ4BinaryCompressor implements Compressor {

  static final LZ4Factory factory = LZ4Factory.fastestInstance();

  static final LZ4SafeDecompressor decompressor = factory.safeDecompressor();

  static final LZ4Compressor fastCompressor = factory.fastCompressor();

  private final LZ4Compressor compressor;

  public LZ4BinaryCompressor(int level) {
    compressor = level == 0 ? fastCompressor : factory.highCompressor(level);
  }

  @Override
  public void compress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    compressor.compress(src, dest);
  }

  @Override
  public void decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    decompressor.decompress(src, dest);
  }

  @Override
  public int maxCompressedLength(int origSize) {
    return compressor.maxCompressedLength(origSize);
  }
}
