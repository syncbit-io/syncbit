# Distributed ML Dataset Architecture Design

# Overview

This document outlines the design for a distributed data architecture aimed at efficiently distributing large machine learning (ML) datasets – training data and models – across a cluster into local storage. The goal is to minimize download times and optimize resource utilization by enabling peer-to-peer (P2P) data sharing and smart caching, similar to a torrent system. Ultimately, this system will make datasets available quickly for GPU training and inference jobs running in the cluster, enabling more efficient deployment of new data/jobs/nodes, especially when the full set of available possible files is larger than a single node’s local storage.

# Context and Proposal

ML models can be very large (700GiB+ for the largest LLMs) and training datasets are often much larger (sometimes hundreds of TiB). The Nvidia DGX reference architecture specifies that each GPU worker machine in a cluster should have a lot of local storage as additional NVMe drives (generally at least 30TB total), with the intention to shard a large dataset into that storage, then load that portion of data into the GPUs locally. The process of loading into GPU VRAM from local drives is significantly faster than over any kind of network storage system.

The same is true when it comes to models; they also suffer from much longer loading times when streaming the data files across network storage. One difference is that a single model generally fits into the local storage we deploy, but all models that are available to potentially load into the cluster combined may not fit.

Many large-file storage options exist both in-cluster and externally, such as NFS backed by a storage array (Intelliflash, DDN, VAST, etc), S3-compatible object storage (local or remote), HTTP, and Git LFS hosted repositories. However, the external sources often have rate-limiting either in transfer throughput or request rate – especially for the same file if you are attempting to download it from multiple nodes at the same time. This particularly applies to very common options such as CloudFlare’s R2 buckets and HuggingFace’s repos, which have very aggressive limits on the same file and/or same client IP within a short period of time.

This can become the primary bottleneck when relying on streaming data from external storage, especially if you need the same datasets on multiple nodes. And if you need more storage total than is available on each node, often the only option is a large cluster storage-array and network filesystem of some kind, but this approach has its own drawbacks and limitations.

Cluster storage-arrays and network filesystems are a decent middle-ground and therefore a common option when needing to serve a large amount of data quickly to a large number of nodes. Products from companies like DDN and VAST that are common in ML clusters quote throughput maximums like 520GB/s sustained read over NFS for the VAST 20x10 Ceres 338 storage cluster. However, that’s in aggregate total, and requires a fixed distribution of 72(\!) unique IPs across your cluster for the NFS mounts to achieve, and even then the write speed is an order of magnitude less (70GB/s for the above product).

Furthermore, there is a fundamental issue with efficiently loading data from a network filesystem mount into GPU memory – the most-common and efficient loading libraries utilize memory-mapping (mmap) of files in order to achieve a level of zero-copy transfer, bypassing disk-\>CPU RAM and skipping the extra copy from there to GPU. The problem is, this requires low-level file locking, which is implemented by the filesystem driver, and in the case of NFS the locks are distributed over the network by necessity, as there could be many readers and writers across the cluster. This results in significantly worse performance than you would otherwise expect based on the available storage system bandwidth. The DGX reference architecture actually calls for storing all of the training data in shared storage, but writing it in shards to local before loading it into the GPUs, for exactly these reasons.

In contrast, modern ML clusters are equipped with 100-400Gbps ethernet bonds, on switching fabrics that are usually 1:1 or 2:1 subscribed backplanes. Meaning that even with a 100Gbps internet uplink, a single pair of nodes can meet or exceed the potential maximum north-south (remote) bandwidth. And given common subscription ratios, we can generally achieve a very high percentage of east-west (node to node) theoretical line-speed transfer simultaneously across dozens or hundreds of nodes. Compared to the VAST quoted theoretical-maximum 520GB/s aggregate read bandwidth, it would only take 20 nodes of 200Gbps ethernet links to equal that.

Additionally, without the shared filesystem abstraction, a lot of the complexities and reasons shared storage arrays exist become unnecessary except as bulk offload, and thus also enables substantially faster aggregate write speeds than those systems can achieve, since node local-storage reading and writing do not require coordination with remote consumers in the same way.

Thus I propose by utilizing the local storage that’s already there and high-speed in-cluster network to implement a peer-to-peer distribution layer, a local storage-array is no longer a hard requirement, as remote providers can still seed the cluster initially, and if there is a local array, improve subsequent re-seeding. This allows for much more flexibility in cluster location, provider, infrastructure and operations.

# Problem Statement

In order to respond to changing demand for large data files on cluster nodes and enable efficient loading into GPUs, we need to address the following:

* Enable requesting that a particular dataset is needed on a particular set of nodes
* Efficiently copy the dataset that a node requires to local storage
* Manage the lifecycle of the dataset
  * Remove unnecessary files to ensure room for new ones as needed
  * Specify what revision of a dataset is needed
  * Detect drift from the authoritative source and retrieve/distribute the changes as desired
* Avoid downloading the same files multiple times from external storage if we must retrieve them
* Respond quickly to changing demand as a result of new jobs, job scaling, job replication, node loss/job rebalancing, and addition of new datasets

# Key Goals

* **Maximize Data Delivery:** Get requested files onto local storage on all designated nodes in the cluster as quickly as possible
* **Minimize Redundant Downloads:** Avoid downloading the same file from external sources more than once unless it's not present anywhere in cluster local-storage
* **Optimize Resource Use:** Leverage P2P transfers to reduce disk/network load on any single node and maximize transfer speeds
* **Reconciliation:** Ensure distribution completion even if individual nodes experience transient failures via persistent state

# System Components

## API Server

* **Job Scheduling:** The API server acts as the central authority, assigning jobs to workers (i.e., which files to download)
* **Peer Discovery:** It maintains a record of which workers possess which files, allowing workers to discover peers for P2P transfers
* **Worker Communication:** Workers communicate with the API to receive job assignments and report their status
* **Intelligence Hub:** This is where the core logic resides to decide which files need to be synced where and when

## Data Workers

* **Data Acquisition:** Workers are responsible for downloading and storing the necessary datasets locally
* **Local Caching:** Each worker implements a disk-backed cache in memory for optimizing read-after-write
* **P2P Sharing:** Workers run a lightweight HTTP server to share cached data to other workers
* **Job Acceptance:** Workers accept jobs from the API

# Worker Synchronization Mechanism

## Block-Level File Access

* Files are divided into fixed-size logical blocks
* Allows for granular caching and sharing
* Easy to map block indexes to file offsets with seeking for read/write via block\_size \* block\_index

## Least Frequently/Recently Used (LFRU) Write-Through/Read-Through Cache

* The local cache operates as a LFRU cache with a RAM limit; the least accessed blocks are evicted upon hitting the limit according to a frequency and recency metric
* The write-through design persists each completed block to disk immediately
* If a block is requested from the cache and isn’t already present, it will be read from disk through the cache as it’s returned to the requester for faster subsequent reads
* All read and write operations on files go through the block-level cache, decoupling it from the local storage implementation

## Local Metadata

* Each worker tracks the state and size of each block, as well as overall file metadata, in a persistent local metadata-store kept with each dataset
* Enables fault tolerance upon worker failure/restart
* Reported to the API upon state change to inform scheduling

## P2P Protocol

* Peers only request and serve blocks from their local caches by referencing {dataset\_name}/{file\_path}/{block\_index}
* No need for managing multipart calculations and Range requests, block indexes are provided by the cache

## Reconciliation Loop

### Job Assignment

* Workers query the API for jobs assigned to them and accept the jobs
* Jobs define what dataset(s) should be on the worker node

### P2P Transfer

* When a worker needs a file, it first queries the API for a list of peers that already have blocks from the file completed
* If available, the worker downloads and caches those blocks directly from peers

### Source Fallback

* If no peer has any completed blocks, a worker coordinates with the API to check if any peers are currently downloading that file
* If any are, the worker skips that file until the next loop cycle to avoid duplicating remote transfers
* If none are and the worker is assigned to download it, it uses its provider interface to perform the transfer, reporting the progress and result to the API
* Each worker has a (likely shared) local per-provider config to define rate limits, concurrency, authentication, and protocol (HTTP, S3, NFS, etc)

# Performance Benefits

## Faster read-after-write

* If multiple nodes need the same file blocks, they can be quickly shared via P2P without needing to read a requested block from the disk repeatedly while writing new ones
* Improves IOPS and throughput for local storage

## Parallel downloads

* Clients can download different blocks of a file from multiple peers simultaneously
  * Opportunity to leverage write-coalescing if blocks are out-of-order for better batching through storage driver
* Improves bandwidth utilization without hotspotting any particular node or network path
* Scales well with node count

## In-memory caching

* Heavily requested blocks stay in memory longer
* Scales with node count extremely well
  * If a large number of nodes all need the same file it will have most blocks available from peer memory caches most of the time during synchronization
* The more nodes that need a block, the more that will have it to distribute to others
* Even further reduces storage device IOPS/throughput limits given we can expect a high ratio of cache hits during a sync cycle as blocks move through the network

## Peer-to-peer

* P2P file transfer networks are well-known for their ability to massively accelerate data throughput across huge numbers of nodes
  * Achieved through a mixture of distributing both network traffic and disk throughput required
* Bandwidth utilization can be rate-limited locally as desired to prevent over-saturation of the ethernet fabric to maintain QoS of other services on the nodes
  * Every peer can decide how many peers and what data rate is appropriate for them with simple reader-side limiters
* The most popular protocol for this is bittorrent, but it carries with it a lot of complexity and design choices based on anonymous, untrusted, transient participants
  * We can remove a lot of that complexity since we can make a lot of assumptions about the nature of the cluster network and peers

# Job Management and Fault Tolerance

## Job Aging

* The API tracks the progress of jobs reported by workers and worker health via heartbeats and status updates
* Worker health therefore is easy to determine at any point in time
* If a worker starts downloading a file but then fails (due to network issues, crashes, etc.), the API server detects this and re-schedules the job
* Prevents downloads from getting stuck and ensures all required data eventually gets distributed

# Summary

This distributed architecture uses a combination of an API server, worker clients with local storage, memory caching, and peer-to-peer file sharing to efficiently and robustly distribute ML datasets across a cluster on-demand.
