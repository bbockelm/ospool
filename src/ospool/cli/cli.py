
import click
import classad
import htcondor

from ospool import __version__


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version=__version__)
def ospool():
    """
    Tools for querying the OSPool about pilot and payload jobs
    """

class EntryType(click.ParamType):

    def shell_complete(self, ctx, args, incomplete):
        print(ctx)
        print(args)
        collector = htcondor.Collector("flock.opensciencegrid.org")
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

@click.command()
@click.option("--output", default="human", help="Output formats")
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.")
@click.option("--factory", default="OSG", help="Name of OSG facctory.")
@click.argument("entry", type=EntryType())
def show_pressure(pool, output, entry, factory):
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
            'GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry'   # Set when a limit is hit at the factory due to per-entry held limit
        ]
    )

    if not entries:
        print(f"No data found for entry {entry}; does it exist?")
        return

    click.echo("\nData for entry " + click.style(f"{entry}\n", bold=True))

    if entries[0].get('GLIDEIN_In_Downtime') == 'True':
        click.echo(click.style("WARNING:", fg='red', bold=True) + " Entry point is currently in downtime\n")

    for entry in entries:
        click.echo("Data for OSPool group " + click.style(f"{entry['GlideGroupName']}", bold=True))

        if 'GlideClientLimitTotalGlideinsPerEntry' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to per-entry limit on total glideins: {entry['GlideClientLimitTotalGlideinsPerEntry']}")
        if 'GlideClientLimitIdleGlideinsPerEntry' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to per-entry limit on idle glideins: {entry['GlideClientLimitIdleGlideinsPerEntry']}")
        if 'GlideClientLimitTotalGlideinsPerGroup' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to group limit on total glideins: {entry['GlideClientLimitTotalGlideinsPerGroup']}")
        if 'GlideClientLimitIdleGlideinsPerGroup' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to group limit on idle glideins: {entry['GlideClientLimitIdleGlideinsPerGroup']}")
        if 'GlideClientLimitTotalGlideinsPerFrontend' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to frontend limit on total glideins: {entry['GlideClientLimitTotalGlideinsPerFrontend']}")
        if 'GlideClientLimitIdleGlideinsPerFrontend' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to frontend limit on idle glideins: {entry['GlideClientLimitIdleGlideinsPerFrontend']}")
        if 'GlideClientLimitTotalGlideinsGlobal' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to pool-wide limit on total glideins: {entry['GlideClientLimitTotalGlideinsGlobal']}")
        if 'GlideClientLimitIdleGlideinsGlobal' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to pool-wide limit on idle glideins: {entry['GlideClientLimitIdleGlideinsGlobal']}")
        if 'GlideFactoryMonitorStatus_GlideFactoryLimitTotalGlideinsPerEntry' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on total entry glideins: {entry['GlideFactoryMonitorStatus_GlideFactoryLimitTotalGlideinsPerEntry']}")
        if 'GlideFactoryMonitorStatus_GlideFactoryLimitIdleGlideinsPerEntry' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on entry idle glideins: {entry['GlideFactoryMonitorStatus_GlideFactoryLimitIdleGlideinsPerEntry']}")
        if 'GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry' in entry:
            click.echo(click.style("WARNING:", fg='red', bold=True) +
                f" Requests will be reduced due to factory limit on entry held glideins: {entry['GlideFactoryMonitorStatus_GlideFactoryLimitHeldGlideinsPerEntry']}")

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
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.")
def list_entries(pool):
    collector = htcondor.Collector(pool)
    entries = collector.query(ad_type=htcondor.AdTypes.Any,
                    constraint='MyType =?= "glideresource"',
                    projection=['GlideFactoryName'])
    entry_names = set()
    for entry in entries:
        if 'GlideFactoryName' in entry:
            entry_names.add(entry['GlideFactoryName'].split("@")[0])

    entry_names = list(entry_names)
    entry_names.sort()
    for entry in entry_names:
        print(f"{entry}")


ospool.add_command(list_entries, name="list-entries")
ospool.add_command(show_pressure, name="show")
