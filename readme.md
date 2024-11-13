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
cbt -project phading-dev -instance test-instance createtable SINGLE
cbt -project phading-dev -instance test-instance createfamily SINGLE w:maxversions=1 # watch time live or offline aggregated. Columns follow ${seasonId}#${epiosdeId} or ${seasonId} pattern.
cbt -project phading-dev -instance test-instance createfamily SINGLE a:maxversions=1 # watch time adjusted, multiplied by grade. Columns follow ${seasonId} pattern.
cbt -project phading-dev -instance test-instance createfamily SINGLE t:maxversions=1 # total aggregated. Columns can be watch time or transmitted bytes.
cbt -project phading-dev -instance test-instance createfamily SINGLE c:maxversions=1 # cursor or placeholder
```

## Schema & algorithm

```yaml
# Next d6 or q6
- row:
    key: d1#${date}#${consumerId}
    columns:
      - name: w:${seasonId}:${episodeId} # watch time in ms
        value: number
- row:
    key: q1#${date}#${consumerId}
    columns:
      - name: c:p # empty string as placehold
        value: string
- row:
    key: d2#${month}#${consumerId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: q2#${month}#${consumerId}
    columns:
      - name: c:p # empty string as placehold
        value: string
- row:
    key: d3#${date}#${publisherId}#${consumerId}
    columns:
      - name: w:${seasonId} # watch time in sec
        value: number
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: q3#{date}#${publisherId} or q3#{date}#${publisherId}#${checkpointId}
    columns:
      - name: c:r # cursor to resume aggregation
        value: string
- row:
    key: d4#${date}#${publisherId}#${checkpointId}
    columns:
      - name: w:${seasonId} # watch time in sec
        value: number
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: d5#${month}#${publisherId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
        value: number
- row:
    key: q5#${month}#${publisherId}
    columns:
      - name: c:p # empty string as placehold
        value: string
# Next f5
- row:
    key: f1#${consumerId}#${date}
    columns:
      - name: w:${seasonId} # watch time in sec
        value: number
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: f2#${consumerId}#${month}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: f3#${publisherId}#${date}
    columns:
      - name: w:${seasonId} # watch time in sec
        value: number
      - name: a:${seasonId} # watch time multiplied by grade in sec
        value: number
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:kb # total transmitted KiB
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

1. Row `q1` with `d1` -> `f1`, `d2`, `q2`, `d3`, `q3`, `d4`
1. Row `q2` with `d2` -> `f2`
1. Row `q3` with `d3`, `d4` -> `q3`, `d4`, `f3`, `d5`
1. Load `q5`
1. Row `q5` with `d5` -> `f4`
