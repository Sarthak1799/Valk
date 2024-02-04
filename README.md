# Valk
A Persistent KV-Store built in Rust.

A persistent, NoSql database with LSM-Tree based architecture for primarily write-heavy loads. Built a Trie based data partitioning mechanism for SSTs. Increased compaction speed by including support for multi-threading and parallelism.
Read queries sped up by independent, parallel processing for child-node tables. A WriteAheadLog maintained for recovery with a separate, custom garbage collector.

## APIs
- Set : Sets a KV pair in the database.
  - Insert: Inserts new a KV pair
  - Update: Updates an KV existing pair
  - Delete: Deletes an KV existing pair from the database
- Get : Fetches a KV pair if it exists in database.
- Scan : Fetches a range of KV pairs from the database. (TODO)

## Components
- API handler
- Storage Engine
- Value-Log
- Garbage collector (In-progress)
- Recovery module (TODO)


### API Handler
This is the client facing API handler. It is meant to take in single or batch request and pass them on to the Storage Engine for processing. Supports all Get, Set and Scan APIs.

### Value-Log
The Log file serves not only as a WriteAheadLog (WAL) but the source of data itself. It's an append only file meant for faster writes. The Storage Engine handles appends to this file. A new record appended to the file becomes the main source of truth for the data. This record is then later queried by the Storage Engine as and when needed. The VLog record is of the following structure - 
```
Log Header
 - Page_id
 - Offset
 - Size
 - Timestamp
Log
 - Key
 - Value
 - RecordType
   - AppendOrUpdate
   - Delete
```

### Storage Engine
The engine consists of an in-memory Log Manager (analogous to a MemTable) and a persistent SST files based on the LSM-Tree Architecture.
- **Log Manager** : The Log Manger is responsible for appending to the Log file. All API requests are first directed to the Log Manager. The structure consistes of an in-memory Sorted Tree based table that houses 
  the latest KV pairs as they come in. When a threshold size limit is reached, a new instance of the table is created for continuing operations while the original table is flushed to disk (i.e KV pairs appended 
  to the log file).
- **Bucket Manager** : The Trie based structure of the LSM-Tree consists of bucket nodes, each of which houses several SST files. Each bucket node consists of a fixed number of children nodes which further houses 
  the next level of inter-dependent SST files (More to be explained in the architecture section). The Bucket Manager controls the functioning of individual bucket nodes and along with the parent structure of 
  Bucket Controller, is responsible for persistence of KV pairs on SST files and the execution of the Compaction process.

### Garbage Collector
A custom file based garbage collector is needed for cleaning up the unused parts of the Log file. Since the Log file is append only, the data stored towards the start of the file is bound be outdated as more new data comes in that updates the exisiing KV pairs. The garbage collector is supposed to free up the space not used by the file anymore by punching holes in the log file.

### Recovery module
