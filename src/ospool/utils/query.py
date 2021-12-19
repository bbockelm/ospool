"""Common queries against the OSPool."""

import fnmatch

import htcondor

import ospool.utils.config as config


# All the projection attributes needed to print out an entry's info.
entry_info_projection = [
    'GlideGroupName',                          # Group name within the frontend
    'GlideFactoryName',                        # The tuple of (entry name, 'gfactory_instance' (not clear if this is static or not), factory)
    'GlideClientMonitorGlideinsRunning',       # Pilots running from this group
    'GlideClientMonitorJobsIdle',              # Idle Jobs in this group matching the entry
    'GlideClientMonitorJobsRunningHere',       # Jobs in this group running on the entry
    'GlideClientMonitorGlideinsIdle',          # Glideins that have started - but are not occupied - that were triggered by the group
    'GlideClientMonitorGlideinsRequestIdle',   # Requested number of glideins idle in the entry for this group.
    'GlideClientMonitorGlideinsRequestMaxRun', # Requested maximum number of glideins in the entry for this group.
    'GlideFactoryMonitorRequestedIdle',        # Number of queued and idle glideins the factory is trying to maintain (after applying per-entry limits)
    'GlideFactoryMonitorRequestedMaxGlideins', # Maximum number of glideins the factory is trying to submit to the entry.
    'GlideFactoryMonitorStatusIdle',           # Number of idle glideins created by the factory
    'GlideFactoryMonitorStatusRunning',        # Number of glideins created by the factory reported running by CE.
    'GlideFactoryMonitorStatusPending',        # Number of idle glideins created by the factory and idle in the CE's queue.
    'GlideFactoryMonitorStatusHeld',           # Number of glideins created by the factory and held at the factory.
    'GLIDEIN_In_Downtime'                      # Whether or not the entry is in downtime.
    'GlideClientLimitTotalGlideinsPerEntry',   # Set when a limit is hit due to total glideins per entry.
    'GlideClientLimitIdleGlideinsPerEntry',    # Set when a limit is hit due to idle glideins.
    'GlideClientLimitTotalGlideinsPerGroup',   # Set when a limit is hit due to total glideins in the group
    'GlideClientLimitIdleGlideinsPerGroup',    # Set when a limit is hit due to idle glideins in the group
    'GlideClientLimitTotalGlideinsPerFrontend',# Set when a limit is hit due to total glideins in the frontend
    'GlideClientLimitIdleGlideinsPerFrontend', # Set when a limit is hit due to idle glideins in the frontend
    'GlideClientLimitTotalGlideinsGlobal',     # Set when a limit is hit due to total glideins in the pool
    'GlideClientLimitIdleGlideinsGlobal',      # Set when a limit is hit due to total glideins in the pool
    'GlideFactoryMonitorStatus_GlideFactoryLimitTotalGlideinsPerEntry', # Set when a limit is hit at the factory due to per-entry limit
    'GlideFactoryMonitorStatus_GlideFactoryLimitIdleGlideinsPerEntry',  # Set when a limit is hit at the factory due to per-entry idle limit
    'GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry',  # Set when a limit is hit at the factory due to per-entry held limit
    'GLIDEIN_CPUS',                            # Describes the CPU configuration of the entry point.
    'GLIDEIN_ESTIMATED_CPUS',                  # If CPUs is set to 'auto' (whole nodes), an estimate of the number of cores per glidein
    'GLIDEIN_Gatekeeper',                      # The hostname of the CE.
    'GLIDEIN_MaxMemMBs',                       # Maximum amount of memory per glidein.
    'GLIDEIN_ResourceName',                    # The OSG resource name of the CE.
    'GLIDEIN_Resource_Slots',                  # Slot layouts
]


class EntryFilter(object):

    def __init__(self, resource=None, gpus_only=False, ce_hostname=None, entry=None, factory=None):
        self.resource = resource.lower() if resource else None
        self.gpus_only = gpus_only
        self.ce_hostname = ce_hostname.lower() if ce_hostname else None
        self.entry = entry.lower() if entry else None
        self.factory = factory.lower() if entry else None

    def __call__(self, entry):
        if self.gpus_only:
            if 'GLIDEIN_Resource_Slots' not in entry:
                return False
            has_gpu = False
            for resource_command in entry['GLIDEIN_Resource_Slots'].split(";"):
                if resource_command.split(",")[0] == 'GPUs':
                    has_gpu = True
                    break
            if not has_gpu:
                return False

        factory = entry['GlideFactoryName'].rsplit("@", 1)[-1].lower()
        if self.factory is not None and factory != self.factory:
            return False

        entry_name = entry['GlideFactoryName'].split("@", 1)[0].lower()
        if self.entry is not None and not fnmatch.fnmatch(entry_name, self.entry):
            return False

        if self.resource is not None:
            if 'GLIDEIN_ResourceName' not in entry or not fnmatch.fnmatch(entry['GLIDEIN_ResourceName'].lower(), self.resource):
                return False

        if self.ce_hostname is not None:
            if 'GLIDEIN_Gatekeeper' not in entry:
                return False
            ce_hostname = entry['GLIDEIN_Gatekeeper'].split(' ', 1)[0].lower()
            if not fnmatch.fnmatch(ce_hostname, self.ce_hostname):
                return False

        return True

    def get_projection_attrs(self):
        return set([
            'GlideFactoryName',       # The name of this entry according to the factory
            'GLIDEIN_Resource_Slots', # The layout of any special resource (including GPUs)
            'GLIDEIN_Gatekeeper',     # The 'grid_resource' line to use for pilot submission.
            'GLIDEIN_ResourceName',   # The OSG topology resource name.
        ])


def query_entries(pool, filter_obj, projection):

    config.add_pool_history(pool)

    collector = htcondor.Collector(pool)
    projection_attrs = set(projection).union(filter_obj.get_projection_attrs())
    entries = collector.query(ad_type=htcondor.AdTypes.Any,
                    constraint='MyType =?= "glideresource"',
                    projection=list(projection_attrs))

    for entry in entries:
        if filter_obj(entry):
            yield entry
