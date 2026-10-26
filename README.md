# MapReduce Word Count

A small MapReduce library in C, with a word-count example. The framework runs mapper and reducer jobs on a pthread worker pool, routes emitted key/value pairs into hash partitions, and reduces each partition into output files.

## Build

Requirements: GCC, Make, and POSIX threads.

```sh
make
```

This builds the `wordcount` executable. Remove generated object files and the executable with:

```sh
make clean
```

## Run the word counter

Provide one or more input files as arguments:

```sh
./wordcount sample_inputs/sample1.txt sample_inputs/sample2.txt
```

The example mapper splits each input on whitespace and emits each token with the value `1`. The reducer counts values for each token. Results are written in the current directory to `result-<partition>.txt`; each line has the form `word: count`.

The example uses five worker threads and ten partitions. Output files are opened in append mode, so remove old results before rerunning if you want a fresh output:

```sh
rm -f result-*.txt
./wordcount sample_inputs/sample*.txt
cat result-*.txt
```

## Framework API

The public declarations are in `mapreduce.h`:

- `MR_Run(file_count, file_names, mapper, reducer, num_workers, num_parts)` starts the framework for the supplied callbacks.
- `MR_Emit(key, value)` emits a key/value pair from a mapper.
- `MR_GetNext(key, partition_idx)` returns the next value for a key during reduction, or `NULL` when no values remain.
- `MR_Partitioner(key, num_partitions)` hashes a key to a partition index.

Mapper and reducer callback types are `Mapper` and `Reducer`, respectively. See `distwc.c` for a complete word-count example; the thread-pool and partition table structures are declared in `threadpool.h`.

## Scheduling and synchronization

The thread pool keeps pending jobs in ascending priority order. Mapper jobs use input file size as priority, and reducer jobs use partition size. The partition table hashes keys into buckets and stores each bucket's linked list in ASCII key order.

The thread pool mutex protects queue and worker state. Its condition variable lets the main thread wait for active work to finish between mapping and reducing without busy waiting. A separate partition table mutex protects bucket access while mapper and reducer threads emit or retrieve values. The framework uses pthread mutex and condition variable initialization and destruction as part of its lifecycle.

## Sample inputs

`sample_inputs/` contains the provided word-count inputs, plus two small `mysample` files. There is no separate automated test runner in this repository; the `wordcount` command can be run against these files as shown above.

## References

The implementation was developed with reference to the pthread and `stat` manual pages, and Stack Overflow discussions about condition variable usage.

