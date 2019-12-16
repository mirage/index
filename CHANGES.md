# Unreleased

## Removed

 - Remove `force_merge` from `Index.S`, due to difficulties with guaranteeing
   sensible semantics to this function under MRSW access patterns. (#147, #150)

# 1.0.1 (2019-11-29)

## Added

- Provide a better CLI interface for the benchmarks (#130, #133)

## Fixed

- Fix a segmentation fault when using musl <= 1.1.20 by not allocating 64k-byte
  buffers on the thread stack (#132)
- Do not call `pwrite` with `len=0` (#131)
- Clear `log.mem` on `close` (#135)
- Load `log_async` on startup (#136)

# 1.0.0 (2019-11-14)

First stable release.
