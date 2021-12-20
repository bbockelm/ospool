# Open Science Pool Information Tool

The Open Science Pool (OSPool) is a part of the
[OSG fabric of services](https://osg-htc.org) and provides
users and groups with the ability to utilize cycles donated
to the OSG.

The `ospool` tool is a human-friendly, terminal based way
to query information about how effectively the OSPool is using
the resources its been given.  It has two modes:

```
ospool list-entries
```

provides a list of "entries" defined in the OSG GlideinWMS
factory and utilized by the OSPool.  Each entry is a combination of
a CE and pilot submission settings (e.g., queue name, amount of resources
requested).

```
ospool show <ENTRY>
```

shows current utilization information about the entry `<ENTRY>`; if `<ENTRY>`
is a Unix glob (e.g., uses the `*` as a wildcard), it will show all matching
entries.

Both the `show` and `list-entries` commands provide a number of filters based
on other attributes of the entry:

   - `--resource`: Shows only entries from a given OSG topology resource.
     Accepts wildcards such as `*CHTC*`.
   - `--ce-hostname`: Show only entries from a given CE hostname.  Accepts
     wildcards such as `*.wisc.edu`.
   - `--gpus-only`: Only show resources providing GPUs.

## Examples

To list all the entries associated with CHTC resources:

```
$ ospool list-entries --resource *CHTC*
OSG_CHTC-ITB-SLURM-CE
OSG_CHTC-canary2
OSG_HOSTED-CE-CHTC-UBUNTU
OSG_HOSTED-CE1-OSGDEV-CHTC
OSG_US_CHTC-LHCB
```

To show information about the `Glow_US_Syracuse3_condor_gpu` entry:

```
$ ospool show Glow_US_Syracuse3_condor_gpu

Data for entry Glow_US_Syracuse3_condor_gpu (Resource name SU-ITS-CE3, CE hostname its-condor-ce3.syr.edu)
- Entry has 4 cores per glidein
- Entry has 1 GPU per glidein

Data for OSPool group gpu-syracuse
- Matching payload jobs:
  - Idle:                          1012
  - Running at this entry:         0
- Requests for glideins in the CE:
  - Idle in CE queue:              30
  - Limit:                         1221
    - WARNING: Factory changed this to 1236
- Created glideins for the CE:
  - Created and idle in factory    29
  - Idle in the CE's queue         29
  - In an error state ("held")     0
  - Reported by CE as running      52
- Running glideins for this group connected to OSPool collector:
  - Slots in collector:            0
  - Slots without payloads:        9
```
