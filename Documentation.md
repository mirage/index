# Index: overview of what it is and how it works



Index is essentially a key-value store. The main functionality is described in the file `src/index_intf.ml`. However this file does not describe the implementation in much detail. The purpose of this document is to fill that gap.



## Initial comments

Index is parameterized by various things. The most important point is that **keys and values must be fixed size**. And, indeed, in order for things to run smoothly the keys and values must be reasonably small. The Tezos instance, for example, uses 32 bytes for the keys and 3 integers (or similar) for the values. So, Index is not a generic key-value store (although it would be fairly simple to build such a thing on top of index). 

Further, keys must be hashable (with the hash represented as an int), and the implementation even requires that the user specify the number of bits in the hash that are relevant.

FIXME There is a worrying comment regarding the key hash: "underestimation [of the number of relevant bits] will result in undefined behavior"; most code that uses hashes should still work (albeit very slowly) even if all hashes have the same value. So, this comment is a bit unusual. What happens if hashes collide? What do we do about this? 

Apart from these restrictions on keys and values, the interface exposed by Index is fairly typical of key-value stores: 

* Basic operations: find, mem, replace (note, no delete operation; replace functions as add)
* Slightly unusual operation: clear
* Traversals: filter, iter
* Low-level syncing: sync, flush
* Unusual merge operations: is_merging, merge, try_merge

The merge operations relate to the internal implementation of Index, which we now discuss further.



## Internal operation of Index

As a rough approximation, Index functions as follows:

* New key-value entries are added to the end of a log file; the contents of the log file is also kept as a hashtable in memory.
* When the log gets "big", it is merged into the index proper. **Merging takes place asynchronously** (in a separate OS thread, and there is some contention on the OCaml runtime lock... so there is some interference with the main thread, and perhaps the two threads could be better balanced). While merging is taking place, new entries are placed in a "log_async" file, which eventually gets renamed over the original log file (when the merge completes and the original log is no longer needed).
* The index proper (or, index/data) is a single (usually large) file which contains all the key-value entries, **sorted by the hash of the key**.
* There is an in-memory **fan** which provides fast lookup within the index data. Essentially it provides a function `search` which takes an integer hash (of the desired key) and returns a pair of (lo,hi) offsets within the data file (i.e., it indicates the part of the file that contains the relevant key-value, if there is an entry for the key at all). The fan is usually constructed as part of the merge.

The merge then consists of creating a new data file, with additional entries. At the moment, this is done by scanning the data file from the beginning, copying entries to a new data file, and inserting (or replacing!) the additional entries from the in-memory log hashtable. When this is finished, the new data file is renamed over the old data file. 

A drawback of this scheme is that as the data becomes larger, the time to merge becomes correspondingly larger. However, the scheme is remarkably effective for even quite large data, because sequential reading and writing of files is extremely well-optimised on modern systems.



## Blocking merges

Eventually, the index data gets sufficiently large that the "log_async" becomes full before the merge completes. At this point Index will block, waiting for the merge of the log to complete. When the merge completes, the full log_async causes another merge to be initiated. Thus, in a high write scenario, with a large data file, merges will be running most of the time, and there will be periodic blocking, waiting for a merge of the log to complete when the log_async is full.



## Correctness

Some care is taken to try to ensure that the code functions correctly even in the event of a system crash or similar. We consider some of the files involved, and what measures are taken.

**Log files:** Note that these files are updated by appending data to the end. Reads can occur at arbitrary offsets, but writes add data at the end of the file. The log and log_async use the "IO" interface `io_intf.ml`, which is a model of the underlying filesystem (it includes eg a function "rename" to rename a file).  For files, operations are: create (v, v_readonly), get "offset" (which is actually the offset at which new data will be placed when flushed from the buffer), read from offset, and get/set header. 

The header includes a "generation" and an "offset".

Data can be buffered, hence the size of the underlying file (the "raw" file) can be less than the size of the in-memory data.

FIXME not clear what header (int63) is; the offset at the end of the header info? Need to get someone to walk me through the code in index_unix.ml

