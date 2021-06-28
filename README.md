# expKV
- Course project for CSCI5570

## Dependency
- Hardware: Two Mellanox HCA connected with a DAC cable.
- Software:
    - Ubuntu 20.04 (Kernel: 5.8.0-41-generic)
    - Mellanox OFED: MLNX_OFED_LINUX-5.2-1.0.4.0 or later

## Quick Start
### Compilation
```bash
git clone https://github.com/bernardshen/expKV.git myKV && cd myKV
makdir build && cd build
cmake ..
make
```
### Running
```bash
cd myKV/build/src
./MyKVServer [TableType] # TableType = [simple, cuckoo, hopscotch]
./MyKVShell [IPaddress] [TableType]
```
Then you will start a MyKV server and a shell that can execute get, put and delete operations (type help for more detail in the shell).
