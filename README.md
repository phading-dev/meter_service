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

## Schema & algorithm

Column families:

1. `w`: Watch time live or offline aggregated or network transmission bytes while watching. Columns follow ${seasonId}#${epiosdeId}#w or ${seasonId}#${epiosdeId}#n or ${seasonId}#${epiosdeId} or ${seasonId} pattern.
1. `a`: Watch time adjusted, multiplied by grade. Columns follow ${seasonId} pattern.
1. `s`: Storage bytes or timestamps of start or end. Columns follow ${r2Dirname}#s/b/e pattern, such that each info related to the same dir is grouped together.
1. `u`: One-time uploaded bytes. Columns follow ${gcsFilename} pattern.
1. `t`: Total aggregated. Columns can be watch time or transmitted bytes or storage or uploaded bytes.
1. `c`: Cursor or placeholder.

```yaml
# Task and paylaod (temporray data). Next d7 and t7
- row:
    key: d1#${date}#${consumerId}
    columns:
      - name: w:${seasonId}#${episodeId}#w # watch time in ms
        value: number
      - name: w:${seasonId}#${epiosdeId}#n # Network transmitted bytes
        value: number
- row:
    key: t1#${date}#${consumerId}
    columns:
      - name: c:p # empty string as placehold
        value: string
- row:
    key: d2#${month}#${consumerId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
- row:
    key: t2#${month}#${consumerId}
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
      - name: t:n # total transmitted KiB
        value: number
- row:
    key: t3#{date}#${publisherId} or t3#{date}#${publisherId}#${checkpointId}
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
      - name: t:n # total network transmitted KiB
        value: number
- row:
    key: d6#${date}#${publisherId}
    columns:
      - name: u:${gcsFilename} # Uploaded bytes
        value: number
      - name: s:${r2Dirname}#b # Stored bytes
        value: number
      - name: s:${r2Dirname}#e # The end timestamp in ms of the day. Optional.
        value: number
      - name: s:${r2Dirname}#s # The start timestamp in ms of the day
        value: number
- row:
    key: t6#${date}#${publisherId}
    columns:
      - name: c:p # empty string as placehold
        value: string
- row:
    key: d5#${month}#${publisherId}#${day}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:n # total network transmitted KiB
        value: number
      - name: t:u # total uploaded KiB
        value: number
      - name: t:s # total storage MiB x min
        value: number
- row:
    key: t5#${month}#${publisherId}
    columns:
      - name: c:p # empty string as placehold
        value: string
# Final servable data. Next f5
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
      - name: t:n # total transmitted KiB
        value: number
      - name: t:u  # total uploaded KiB
        value: number
      - name: t:s # total storage MiB x min
        value: number
- row:
    key: f4#${publisherId}#${month}
    columns:
      - name: t:w # total watch time multiplied by grade in sec
        value: number
      - name: t:n # total transmitted MiB
        value: number
      - name: t:u # total uploaded MiB
        value: number
      - name: t:s # total storage MiB x hour
        value: number
```

Algorithm:

1. Row `t1` with `d1` -> `f1`, `d2`, `t2`, `d3`, `t3`, `d4`
1. Row `t2` with `d2` -> `f2`
1. Row `t3` with `d3`, `d4` -> `t3`, `d4`, `f3`, `d5`, `t5`
1. Row `t6` and `d6` -> `f3`, `d6`, `t6`, `d5`, `t5`
1. Row `t5` with `d5` -> `f4`
