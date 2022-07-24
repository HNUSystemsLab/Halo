# Halo: A Hybrid PMem-DRAM Persistent Hash Index with Fast Recovery

**Halo** is a hybrid PMem-DRAM persistent hash index with fast recovery featuring a specifically designed volatile index and log-structured persistent storage layout.
In order to suppress write amplification caused by memory allocators and to facilitate recovery, we propose **Halloc**, a highly-efficient memory manager for Halo. In addition, we pro- pose mechanisms such as batched writes, prefetching for hybrid reads, and reactive snapshot to further optimize performance.

## Paper
This is the code for our paper **Halo: A Hybrid PMem-DRAM Persistent Hash Index with Fast Recovery**(accepted by SIGMOD '22).

## Dependencies
### For building
#### Required
* `libpmem` in PMDK 
* `libvmem` in PMDK



## How to build
* Call `make` to generate all binaries.

## To generate YCSB workloads
```sh
cd YCSB
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar -xvf ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 YCSB
#Then run workload generator
mkdir workloads
./generate_all_workloads.sh
```
## To generate PiBench workloads
```sh
cd PiBench
make
./auto_gene.sh
```
## How to run
See `autorun.sh`
