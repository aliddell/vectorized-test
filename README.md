# Tests of vectorized vs. consolidated chunk writing

This repository tests the relative performance of vectorized chunk writing vs. the current acquire-zarr scheme, namely,
consolidating a layer or "slab" of chunks in memory and then writing out the entire slab at once.

## Tests

### Single shard

This test generates increasing numbers of chunks of size 128 x 128 x 128, and writes them out as a single shard to a
binary file.
This test is run with both vectorized (`pwritev` on POSIX) and consolidated chunk writing, and the time taken for each
write of each size is recorded in a CSV file `results.csv`.