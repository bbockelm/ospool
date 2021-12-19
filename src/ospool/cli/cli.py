
import click
import classad
import htcondor

from ospool import __version__
import ospool.utils.config as config


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version=__version__)
def ospool():
    """
    Tools for querying the OSPool about pilot and payload jobs
    """

class EntryType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):
        collector = htcondor.Collector(ctx.params['pool'])
        entries = collector.query(ad_type=htcondor.AdTypes.Any,
                        constraint='MyType =?= "glideresource"',
                        projection=['GlideFactoryName'])
        entry_names = set()
        for entry in entries:
            if 'GlideFactoryName' in entry:
                entry_names.add(entry['GlideFactoryName'].split("@")[0])

        entry_names = list(entry_names)
        entry_names.sort()
        return [click.shell_completion.CompletionItem(name) for name in entry_names if name.startswith(incomplete)]


class PoolType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):
        return [click.shell_completion.CompletionItem(name) for name in config.get_pool_history() if name.startswith(incomplete)]


@click.command()
@click.option("--output", default="human", help="Output formats")
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.", type=PoolType(), show_default=True)
@click.option("--factory", default="OSG", help="Name of OSG factory.", show_default=True, type=click.Choice(["OSG", "OSG-ITB"], case_sensitive=False))
@click.argument("entry", type=EntryType())
def show_pressure(pool, output, entry, factory):

    config.add_pool_history(pool)

    collector = htcondor.Collector(pool)
    factory_name = f"{entry}@gfactory_instance@{factory}"
    entries = collector.query(ad_type=htcondor.AdTypes.Any,
        constraint='MyType =?= "glideresource" && %s =?= GlideFactoryName' % classad.quote(factory_name),
        projection = [
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
    )

    if not entries:
        print(f"No data found for entry {entry}; does it exist?")
        return

    click.echo("\nData for entry " + click.style(f"{entry}", bold=True))
    if 'GlideClientLimitTotalGlideinsPerEntry' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to per-entry limit on total glideins: {entries[0]['GlideClientLimitTotalGlideinsPerEntry']}")
    if 'GlideClientLimitIdleGlideinsPerEntry' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to per-entry limit on idle glideins: {entries[0]['GlideClientLimitIdleGlideinsPerEntry']}")
    if 'GlideClientLimitTotalGlideinsPerFrontend' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to frontend limit on total glideins: {entries[0]['GlideClientLimitTotalGlideinsPerFrontend']}")
    if 'GlideClientLimitIdleGlideinsPerFrontend' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to frontend limit on idle glideins: {entries[0]['GlideClientLimitIdleGlideinsPerFrontend']}")
    if 'GlideClientLimitTotalGlideinsGlobal' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to pool-wide limit on total glideins: {entries[0]['GlideClientLimitTotalGlideinsGlobal']}")
    if 'GlideClientLimitIdleGlideinsGlobal' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
            f" Requests will be reduced due to pool-wide limit on idle glideins: {entries[0]['GlideClientLimitIdleGlideinsGlobal']}")
    if 'GlideFactoryMonitorStatus_GlideFactoryLimitTotalGlideinsPerEntry' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on total entry glideins: {entries[0]['GlideFactoryMonitorStatus_GlideFactoryLimitTotalGlideinsPerEntry']}")
    if 'GlideFactoryMonitorStatus_GlideFactoryLimitIdleGlideinsPerEntry' in entries[0]:
        click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on entry idle glideins: {entries[0]['GlideFactoryMonitorStatus_GlideFactoryLimitIdleGlideinsPerEntry']}")
    if 'GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry' in entries[0]:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on entry held glideins: {entries[0]['GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry']}")

    if 'GLIDEIN_CPUS' in entries[0]:
        if entries[0]['GLIDEIN_CPUS'] == "auto":
            print("- Whole node entry " + ("with an estimated {} cores per glidein".format(entries[0]['GLIDEIN_ESTIMATED_CPUS']) if 'GLIDEIN_ESTIMATED_CPUS' in entries[0] else ''))
        else:
            print(f"- Entry has {entries[0]['GLIDEIN_CPUS']} cores per glidein")

    print()

    if entries[0].get('GLIDEIN_In_Downtime') == 'True':
        click.echo(click.style("WARNING:", fg='red', bold=True) + " Entry point is currently in downtime\n")

    for entry in entries:
        click.echo("Data for OSPool group " + click.style(f"{entry['GlideGroupName']}", bold=True))

        if 'GlideClientLimitTotalGlideinsPerGroup' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to group limit on total glideins: {entry['GlideClientLimitTotalGlideinsPerGroup']}")
        if 'GlideClientLimitIdleGlideinsPerGroup' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to group limit on idle glideins: {entry['GlideClientLimitIdleGlideinsPerGroup']}")

        print( "- Matching payload jobs:")
        print(f"  - Idle:                          {entry['GlideClientMonitorJobsIdle']}")
        print(f"  - Running at this entry:         {entry['GlideClientMonitorJobsRunningHere']}")
        print(f"- Requests for glideins in the CE:")
        print(f"  - Idle in CE queue:              {entry['GlideClientMonitorGlideinsRequestIdle']}")
        if entry['GlideClientMonitorGlideinsRequestIdle'] != entry['GlideFactoryMonitorRequestedIdle']:
            click.echo("    - " + click.style("WARNING", bold=True, fg='red') + f": Factory changed this to {entry['GlideFactoryMonitorRequestedIdle']}")
        print(f"  - Limit:                         {entry['GlideClientMonitorGlideinsRequestMaxRun']}")
        if entry['GlideClientMonitorGlideinsRequestMaxRun'] != entry['GlideFactoryMonitorRequestedMaxGlideins']:
            click.echo("    - " + click.style("WARNING", bold=True, fg='red') + f": Factory changed this to {entry['GlideFactoryMonitorRequestedMaxGlideins']}")
        print( "- Created glideins for the CE:")
        print(f"  - Created and idle in factory    {entry['GlideFactoryMonitorStatusIdle']}")
        print(f"  - Idle in the CE's queue         {entry['GlideFactoryMonitorStatusPending']}")
        if entry.get("GlideFactoryMonitorStatusHeld", 0):
            click.echo("  - In an error state (\"held\")     " + click.style(f"{entry['GlideFactoryMonitorStatusHeld']}", fg='red', bold=True))
        else:
            print(f"  - In an error state (\"held\")     {entry['GlideFactoryMonitorStatusHeld']}")
        print(f"  - Reported by CE as running      {entry['GlideFactoryMonitorStatusRunning']}")
        print( "- Running glideins for this group connected to OSPool collector:")
        print(f"  - Slots in collector:            {entry['GlideClientMonitorGlideinsRunning']}")
        print(f"  - Slots without payloads:        {entry['GlideClientMonitorGlideinsIdle']}")
        print()

@click.command()
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.", type=PoolType(), show_default=True)
@click.option("--gpus-only", default=False, help="Only show resources with GPUs.", is_flag=True)
def list_entries(pool, gpus_only):

    config.add_pool_history(pool)

    collector = htcondor.Collector(pool)
    entries = collector.query(ad_type=htcondor.AdTypes.Any,
                    constraint='MyType =?= "glideresource"',
                    projection=['GlideFactoryName','GLIDEIN_Resource_Slots'])
    entry_names = set()
    for entry in entries:
        if gpus_only:
            if 'GLIDEIN_Resource_Slots' not in entry:
                continue
            has_gpu = False
            for resource_command in entry['GLIDEIN_Resource_Slots'].split(";"):
                if resource_command.split(",")[0] == 'GPUs':
                    has_gpu = True
                    break
            if not has_gpu:
                continue
        if 'GlideFactoryName' in entry:
            entry_names.add(entry['GlideFactoryName'].split("@")[0])

    entry_names = list(entry_names)
    entry_names.sort()
    for entry in entry_names:
        print(f"{entry}")


ospool.add_command(list_entries, name="list-entries")
ospool.add_command(show_pressure, name="show")
