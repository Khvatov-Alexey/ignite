package org.apache.ignite.internal.binary.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
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
  public int compress(ByteBuffer src, ByteBuffer dest, int destOff, int maxDestLen) throws IOException {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";
    assert destOff < dest.capacity() : "Destination offset more than buffer capacity";
    assert maxDestLen + destOff <= dest.capacity() : "Destination offset and length more than buffer capacity";

    return compressor.compress(src, src.position(), src.remaining(), dest, destOff, maxDestLen);
  }

  @Override
  public int decompress(@NotNull ByteBuffer src, @NotNull ByteBuffer dest) {
    assert src != null : "Source buffer is null";
    assert dest != null : "Destinantion buffer is null";

    decompressor.decompress(src, dest);
    return dest.position();
  }

  @Override
  public int maxCompressedLength(int origSize) {
    return compressor.maxCompressedLength(origSize);
  }
}
