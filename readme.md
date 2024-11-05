## BigTable configuration

Name: product-meter
Instance ID: product-meter
Storage type: SSD
Cluster ID: product-meter-c1
Region: us-west1 (Oregon)
Zone: any
Scaling mode: Autoscaling
Minimum: 1 node
Maximum: 5 nodes
CPU utilization target: 80%
Storage utilization target: 4

## Table creation

```shell
cbt -project test -instance test createtable SINGLE
cbt -project test -instance test createfamily SINGLE w:maxversions=1:intsum # watch time live aggregated. Columns follow ${seasonId}#${epiosdeId} pattern.
cbt -project test -instance test createfamily SINGLE a:maxversions=1 # watch time further aggregated and/or multiplied by grade. Columns follow ${seasonId} pattern.
cbt -project test -instance test createfamily SINGLE t:maxversions=1 # total aggregated. Columns can be watch time or transmitted bytes.
cbt -project test -instance test createfamily SINGLE c:maxversions=1 # cursor or completion
```

## Schema & algorithm

```yaml
# Next t8
- row:
    key: t1#${date}#${consumerId}
    columns:
      - name: w:${seasonId}:${episodeId} # watch time in ms
        value: number
- row:
    key: t2#${month}#${consumerId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: t6#${month}#${consumerId}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: c:p # Whether aggregation is completed. Empty string means false. Otherwise true.
        value: string
- row:
    key: t3#${date}#${publisherId}#${consumerId}
    columns:
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: t4#${date}#${publisherId}
    columns:
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
      - name: c:r # Cursor to resume aggregation
        value: string
      - name: c:p # Whether aggregation is completed. Empty string means false. Otherwise true.
        value: string
- row:
    key: t5#${month}#${publisherId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: t7#${month}#${publisherId}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:mb # total transmitted MiB
        value: number
      - name: c:p # Whether aggregation is completed. Empty string means false. Otherwise true.
        value: string
# Next f5
- row:
    key: f1#${consumerId}#${date}
    columns:
      - name: a:${seasonId} # watch time in sec (not multiplied by grade)
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: f2#${publisherId}#${date}
    columns:
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: f3#${consumerId}#${month}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: f4#${publisherId}#${month}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:mb # total transmitted MiB
        value: number
      - name: t:smbh # storage as MiB x hour
        value: number
      - name: t:umb # uploaded MiB
        value: number
- row:
    key: l1
    columns:
      - name: c:m # ISO string of the month that it's loading publishers.
        value: string
      - name: c:t # Account created timestamp in ms as the cursor
        value: number
```

Algorithm:

1. Row `t1` -> `f1`,`t2`, `t6`, `t3`, `t4`
1. Row `t4` and `t3` -> `f2`, `t5`
1. Row `t6` and `t2` -> `f3`
1. Row `f5` -> `t7`
1. Row `t7` and `t5` -> `f4`
