
import collections
import fnmatch

import click
import classad
import htcondor

from ospool import __version__
import ospool.utils.config as config
import ospool.utils.query as query


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version=__version__)
def ospool():
    """
    Tools for querying the OSPool about pilot and payload jobs
    """


def filter_obj_from_ctx(ctx):

    return query.EntryFilter(gpus_only=ctx.params.get('gpus_only'),
            resource=ctx.params.get('resource'), entry=ctx.params.get('entry_name'),
            factory=ctx.params.get('factory'), ce_hostname=ctx.params.get('ce_hostname'))


class EntryType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):

        filter_obj = filter_obj_from_ctx(ctx)

        entry_names = set()
        for entry in query.query_entries(ctx.params['pool'], filter_obj, []):
            if 'GlideFactoryName' in entry:
                entry_names.add(entry['GlideFactoryName'].split("@")[0])

        entry_names = list(entry_names)
        entry_names.sort()
        return [click.shell_completion.CompletionItem(name) for name in entry_names if name.startswith(incomplete)]


class ResourceType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):

        filter_obj = filter_obj_from_ctx(ctx)

        resource_names = set()
        for entry in query.query_entries(ctx.params['pool'], filter_obj, []):
            if 'GLIDEIN_ResourceName' in entry:
                resource_names.add(entry['GLIDEIN_ResourceName'])

        resource_names = list(resource_names)
        resource_names.sort()
        return [click.shell_completion.CompletionItem(name) for name in resource_names if name.startswith(incomplete)]


class CEHostnameType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):

        filter_obj = filter_obj_from_ctx(ctx)

        ce_hostnames = set()
        for entry in query.query_entries(ctx.params['pool'], filter_obj, []):
            if 'GLIDEIN_Gatekeeper' in entry:
                ce_hostnames.add(entry['GLIDEIN_Gatekeeper'].split(" ", 1)[0])

        ce_hostnames = list(ce_hostnames)
        ce_hostnames.sort()
        return [click.shell_completion.CompletionItem(name) for name in ce_hostnames if name.startswith(incomplete)]


class PoolType(click.ParamType):

    def shell_complete(self, ctx, param, incomplete):
        return [click.shell_completion.CompletionItem(name) for name in config.get_pool_history() if name.startswith(incomplete)]


def print_human_friendly_entry(entry, entries):

    site_info = []
    if 'GLIDEIN_ResourceName' in entries[0]:
        site_info.append(f"Resource name {entries[0]['GLIDEIN_ResourceName']}")
    if 'GLIDEIN_Gatekeeper' in entries[0]:
        site_info.append(f"CE hostname {entries[0]['GLIDEIN_Gatekeeper'].split()[0]}")
    site_info = " (" + ", ".join(site_info) + ")" if site_info else ""
    click.echo("\nData for entry " + click.style(f"{entry}", bold=True) + site_info)
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
        elif entries[0]['GLIDEIN_CPUS'] == "1":
            print(f"- Entry has 1 core per glidein")
        else:
            print(f"- Entry has {entries[0]['GLIDEIN_CPUS']} cores per glidein")

    if 'GLIDEIN_Resource_Slots' in entries[0]:
        for slot_description in entries[0]['GLIDEIN_Resource_Slots'].split(";"):
            slot_info = slot_description.split(",")
            if slot_info[0] != 'GPUs':
                continue
            if len(slot_info) == 3:
                if slot_info[1] == "1":
                    print(f"- Entry has 1 GPU per glidein")
                else:
                    print(f"- Entry has {slot_info[1]} GPUs per glidein")
            else:
                print("- Entry has GPUs available to glidein")

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
        if ('GlideFactoryMonitorRequestedIdle' in entry) and entry['GlideClientMonitorGlideinsRequestIdle'] != entry['GlideFactoryMonitorRequestedIdle']:
            click.echo("    - " + click.style("WARNING", bold=True, fg='red') + f": Factory changed this to {entry['GlideFactoryMonitorRequestedIdle']}")
        print(f"  - Limit:                         {entry['GlideClientMonitorGlideinsRequestMaxRun']}")
        if ('GlideFactoryMonitorRequestedMaxGlideins' in entry) and entry['GlideClientMonitorGlideinsRequestMaxRun'] != entry['GlideFactoryMonitorRequestedMaxGlideins']:
            click.echo("    - " + click.style("WARNING", bold=True, fg='red') + f": Factory changed this to {entry['GlideFactoryMonitorRequestedMaxGlideins']}")
        if 'GlideFactoryMonitorStatusIdle' in entry or 'GlideFactoryMonitorStatusPending' in entry or 'GlideFactoryMonitorStatusRunning' in entry:
            print( "- Created glideins for the CE:")
            if 'GlideFactoryMonitorStatusPending' in entry:
                print(f"  - Created and idle in factory    {entry['GlideFactoryMonitorStatusIdle']}")
            if 'GlideFactoryMonitorStatusPending' in entry:
                print(f"  - Idle in the CE's queue         {entry['GlideFactoryMonitorStatusPending']}")
            if entry.get("GlideFactoryMonitorStatusHeld", 0):
                click.echo("  - In an error state (\"held\")     " + click.style(f"{entry['GlideFactoryMonitorStatusHeld']}", fg='red', bold=True))
            elif 'GlideFactoryMonitorStatusHeld' in entry:
                print(f"  - In an error state (\"held\")     {entry['GlideFactoryMonitorStatusHeld']}")
            if 'GlideFactoryMonitorStatusRunning' in entry:
                print(f"  - Reported by CE as running      {entry['GlideFactoryMonitorStatusRunning']}")
        print( "- Running glideins for this group connected to OSPool collector:")
        print(f"  - Slots in collector:            {entry['GlideClientMonitorGlideinsRunning']}")
        print(f"  - Slots without payloads:        {entry['GlideClientMonitorGlideinsIdle']}")
        print()


@click.command()
@click.option("--output", default="human", help="Output formats")
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.", type=PoolType(), show_default=True)
@click.option("--factory", default="OSG", help="Name of OSG factory.", show_default=True, type=click.Choice(["OSG", "OSG-ITB"], case_sensitive=False))
@click.option("--resource", help="Show only entries from resources matching glob.", type=ResourceType())
@click.option("--ce-hostname", help="Show only entries from CE hostnames matching glob.", type=CEHostnameType())
@click.option("--gpus-only", default=False, help="Only show resources with GPUs.", is_flag=True)
@click.argument("entry_name", type=EntryType(), required=False)
def show_pressure(pool, output, entry_name, factory, resource, ce_hostname, gpus_only):

    filter_obj = query.EntryFilter(entry=entry_name, factory=factory, resource=resource, ce_hostname=ce_hostname, gpus_only=gpus_only)

    entry_info = collections.defaultdict(list)
    has_entry_name = entry_name is None
    for entry in query.query_entries(pool, filter_obj, query.entry_info_projection):
        tmp_entry_name = entry['GlideFactoryName'].split("@")[0]
        if entry_name and fnmatch.fnmatch(tmp_entry_name.lower(), entry_name.lower()):
            has_entry_name = True
        entry_info[tmp_entry_name].append(entry)

    if not has_entry_name:
        print(f"No data found for entry {entry_name}; does it exist?")
        return

    for key, entries in entry_info.items():
        print_human_friendly_entry(key, entries)


@click.command()
@click.option("--pool", default="flock.opensciencegrid.org", help="OSPool collector hostname.", type=PoolType(), show_default=True)
@click.option("--gpus-only", default=False, help="Only show resources with GPUs.", is_flag=True)
@click.option("--resource", help="Show only entries from resources matching glob.", type=ResourceType())
@click.option("--ce-hostname", help="Show only entries from CE hostnames matching glob.", type=CEHostnameType())
@click.option("--factory", default="OSG", help="Name of OSG factory.", show_default=True, type=click.Choice(["OSG", "OSG-ITB"], case_sensitive=False))
@click.argument("entry_name", type=EntryType(), required=False)
def list_entries(pool, gpus_only, resource, factory, ce_hostname, entry_name):

    filter_obj = query.EntryFilter(gpus_only=gpus_only, resource=resource, entry=entry_name, factory=factory, ce_hostname=ce_hostname)

    entry_names = set()
    for entry in query.query_entries(pool, filter_obj, []):
        if 'GlideFactoryName' in entry:
            entry_names.add(entry['GlideFactoryName'].split("@")[0])

    entry_names = list(entry_names)
    entry_names.sort()
    for entry in entry_names:
        print(f"{entry}")


ospool.add_command(list_entries, name="list-entries")
ospool.add_command(show_pressure, name="show")
