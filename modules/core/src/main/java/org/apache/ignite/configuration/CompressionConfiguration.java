package org.apache.ignite.configuration;

import java.io.Serializable;

public class CompressionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Compression enabled. */
    private boolean enabled;

    /**
     * Compression level. {@code 0} to use default.
     * ZSTD: from {@code -131072} to {@code 22} (default {@code 3}).
     * LZ4: from {@code 0} to {@code 17} (default {@code 0}).
     */
    private int compressionLevel;

    /**
     * Compression algorithm.
     * Supported: LZ4, ZSTD or Snappy
     */
    private BinaryObjectCompressionAlgorithm compressionAlgorithm = BinaryObjectCompressionAlgorithm.LZ4;

    /**
     * Gets compression enabled flag.
     * By default {@code true} (use {@code null} CompressionConfiguration as disabled default).
     *
     * @return compression enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets compression enabled flag.
     *
     * @param enabled Compression enabled flag.
     */
    public CompressionConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }


    /**
     * Gets compression level, as interpreted by implementation.
     *
     * @return compression level.
     */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /**
     * Sets compression level, as interpreted by implementation.
     * It is recommended to keep default value or do extensive benchmarking after changing this setting.
     * Note that default is chosen for {@code zstd-jni} library.
     *
     * @param compressionLevel compression level.
     */
    public CompressionConfiguration setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;

        return this;
    }


    /**
     * Gets binary object compression algorithm.
     * Makes sense only with enabled {@link DataRegionConfiguration#setPersistenceEnabled persistence}.
     *
     * @return Binary object compression algorithm.
     * @see #getCompressionLevel
     */
    public BinaryObjectCompressionAlgorithm getCompressionAlgorithm() {
        return compressionAlgorithm;
    }

    /**
     * Sets binary object compression algorithm.
     * Makes sense only with enabled {@link DataRegionConfiguration#setPersistenceEnabled persistence}.
     *
     * @param compressionAlgorithm Binary object compression algorithm.
     * @return {@code this} for chaining.
     * @see #setCompressionLevel
     */
    public CompressionConfiguration setCompressionAlgorithm(BinaryObjectCompressionAlgorithm compressionAlgorithm) {
        this.compressionAlgorithm = compressionAlgorithm;

        return this;
    }
}
