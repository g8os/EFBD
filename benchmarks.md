# Benchmarks

Test setup:
* 1 vm with 1 cpu and 2GB memory on a deduped volume
* Host = g8os with nbdserver on the same Host
* Storagecluster on a different host (g8os) running 8 storage ardb's on ssd and 1 metadata ardb on nvme

## Filesystem fio test:


### Random read write:
```
sudo -S fio --bs=4k --iodepth=4 --direct=0 --ioengine=libaio --size=2000M --readwrite=randrw --rwmixwrite=80 --group_reporting --directory=/mnt/testdir --runtime=300 --numjobs=1 --name=randrwtest

randrwtest: Laying out IO file(s) (1 file(s) / 2000MB)
Jobs: 1 (f=1): [m(1)] [100.0% done] [75KB/298KB/0KB /s] [18/74/0 iops] [eta 00m:00s]
randrwtest: (groupid=0, jobs=1): err= 0: pid=11744: Thu Mar 16 14:27:28 2017
  read : io=68052KB, bw=2266.1KB/s, iops=566, runt= 30019msec
    slat (usec): min=142, max=3812.8K, avg=1515.97, stdev=34359.67
    clat (usec): min=5, max=2001.2K, avg=997.50, stdev=24635.96
     lat (usec): min=156, max=4068.8K, avg=2514.24, stdev=44384.66
    clat percentiles (usec):
     |  1.00th=[   13],  5.00th=[   15], 10.00th=[   18], 20.00th=[   24],
     | 30.00th=[   28], 40.00th=[   33], 50.00th=[   45], 60.00th=[  434],
     | 70.00th=[  510], 80.00th=[  572], 90.00th=[  724], 95.00th=[ 1032],
     | 99.00th=[ 1560], 99.50th=[36096], 99.90th=[78336], 99.95th=[107008],
     | 99.99th=[1646592]
    bw (KB  /s): min=    2, max=10576, per=100.00%, avg=3348.51, stdev=3633.39
  write: io=271468KB, bw=9043.3KB/s, iops=2260, runt= 30019msec
    slat (usec): min=2, max=2000.1K, avg=58.94, stdev=9953.36
    clat (usec): min=10, max=4068.8K, avg=1077.73, stdev=33386.75
     lat (usec): min=12, max=4068.8K, avg=1136.94, stdev=35029.27
    clat percentiles (usec):
     |  1.00th=[   11],  5.00th=[   13], 10.00th=[   14], 20.00th=[   17],
     | 30.00th=[   20], 40.00th=[   22], 50.00th=[   33], 60.00th=[  438],
     | 70.00th=[  506], 80.00th=[  572], 90.00th=[  772], 95.00th=[ 1048],
     | 99.00th=[ 1544], 99.50th=[38656], 99.90th=[81408], 99.95th=[111104],
     | 99.99th=[1695744]
    bw (KB  /s): min=    9, max=43176, per=100.00%, avg=13346.54, stdev=14447.36
    lat (usec) : 10=0.01%, 20=25.10%, 50=25.63%, 100=0.27%, 250=0.66%
    lat (usec) : 500=17.09%, 750=21.15%, 1000=4.01%
    lat (msec) : 2=5.39%, 4=0.05%, 10=0.02%, 50=0.39%, 100=0.19%
    lat (msec) : 250=0.03%, 500=0.01%, 750=0.01%, 2000=0.01%, >=2000=0.01%
  cpu          : usr=0.81%, sys=2.09%, ctx=17128, majf=0, minf=11
  IO depths    : 1=0.1%, 2=0.1%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=17013/w=67867/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=4

Run status group 0 (all jobs):
   READ: io=68052KB, aggrb=2266KB/s, minb=2266KB/s, maxb=2266KB/s, mint=30019msec, maxt=30019msec
  WRITE: io=271468KB, aggrb=9043KB/s, minb=9043KB/s, maxb=9043KB/s, mint=30019msec, maxt=30019msec

Disk stats (read/write):
  vda: ios=17009/43923, merge=0/4, ticks=23164/2498004, in_queue=2523456, util=96.96%
```

### Sequential read write:
```
sudo -S fio --bs=4k --iodepth=4 --direct=0 --ioengine=libaio --size=2000M --readwrite=readwrite --rwmixwrite=80 --group_reporting --directory=/mnt/testdir --runtime=300 --numjobs=1 --name=randrwtest

Jobs: 1 (f=1): [M(1)] [100.0% done] [44966KB/175.9MB/0KB /s] [11.3K/45.2K/0 iops] [eta 00m:00s]
randrwtest: (groupid=0, jobs=1): err= 0: pid=11774: Thu Mar 16 14:42:44 2017
  read : io=410052KB, bw=67576KB/s, iops=16894, runt=  6068msec
    slat (usec): min=0, max=702949, avg=20.59, stdev=2670.84
    clat (usec): min=1, max=702973, avg=39.27, stdev=2396.14
     lat (usec): min=2, max=703171, avg=60.08, stdev=3588.57
    clat percentiles (usec):
     |  1.00th=[    9],  5.00th=[   10], 10.00th=[   10], 20.00th=[   11],
     | 30.00th=[   11], 40.00th=[   12], 50.00th=[   13], 60.00th=[   14],
     | 70.00th=[   15], 80.00th=[   17], 90.00th=[   19], 95.00th=[   23],
     | 99.00th=[   42], 99.50th=[   56], 99.90th=[ 5856], 99.95th=[ 9664],
     | 99.99th=[12096]
    bw (KB  /s): min=14998, max=130008, per=100.00%, avg=70335.55, stdev=34004.75
  write: io=1599.6MB, bw=269932KB/s, iops=67483, runt=  6068msec
    slat (usec): min=1, max=13862, avg= 7.31, stdev=190.65
    clat (usec): min=4, max=703142, avg=34.18, stdev=2007.72
     lat (usec): min=6, max=703146, avg=41.73, stdev=2016.76
    clat percentiles (usec):
     |  1.00th=[    9],  5.00th=[   10], 10.00th=[   10], 20.00th=[   11],
     | 30.00th=[   11], 40.00th=[   12], 50.00th=[   12], 60.00th=[   14],
     | 70.00th=[   15], 80.00th=[   17], 90.00th=[   19], 95.00th=[   23],
     | 99.00th=[   41], 99.50th=[   54], 99.90th=[ 4704], 99.95th=[ 9536],
     | 99.99th=[12096]
    bw (KB  /s): min=59451, max=517512, per=100.00%, avg=280909.27, stdev=135579.71
    lat (usec) : 2=0.01%, 10=4.55%, 20=87.05%, 50=7.77%, 100=0.43%
    lat (usec) : 250=0.03%, 500=0.01%, 750=0.01%, 1000=0.01%
    lat (msec) : 2=0.02%, 4=0.01%, 10=0.07%, 20=0.04%, 250=0.01%
    lat (msec) : 500=0.01%, 750=0.01%
  cpu          : usr=9.30%, sys=32.11%, ctx=1013, majf=0, minf=12
  IO depths    : 1=0.1%, 2=0.1%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=102513/w=409487/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=4

Run status group 0 (all jobs):
   READ: io=410052KB, aggrb=67576KB/s, minb=67576KB/s, maxb=67576KB/s, mint=6068msec, maxt=6068msec
  WRITE: io=1599.6MB, aggrb=269932KB/s, minb=269932KB/s, maxb=269932KB/s, mint=6068msec, maxt=6068msec

Disk stats (read/write):
  vda: ios=297/1474, merge=0/8, ticks=3596/367124, in_queue=370952, util=90.54%
```

## Device fio test:
```
sudo -S fio --bs=4k --iodepth=4 --direct=0 --ioengine=libaio --size=2000M --readwrite=1 --rwmixwrite=80 \
    --group_reporting  --filename=/dev/vdz --runtime=30 --numjobs=1
```
