from __future__ import print_function, division
import signal, sys
import datetime as dt

import click

from .version import __version__

# 30 days ago
default_start_time_threshold = (dt.datetime.now() - dt.timedelta(30)).strftime("%Y-%m-%d %H:%M:%S")
default_end_time_threshold = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    '''bmetrica: A CLI interface to IBM's Plaform LSF backend MySQL/Cacti database.'''
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

@cli.command(short_help="Job statistics on submitted LSF Job IDs")
@click.option('--all', '-a', is_flag=True, default=False,
              help='show all historical LSF job id statistics')
@click.option('--debug', '-d', is_flag=True, default=False,
              help='show SQL query used')
@click.option('--recent', '-r', is_flag=True, default=False,
              help='skip inspecting historical tables')
@click.option('--threshold', '-t', default='1970-01-01 00:00:00',
              show_default=True, type=click.STRING,
              help="only look for jobs after a timestamp threshold")
@click.option('--melt', '-m', is_flag=True, default=False,
              help='display job stats in melted format')
@click.option('--json', '-j', is_flag=True, default=False,
              help='display job stats in JSON format')
@click.option('--parse', '-p', is_flag=True, default=False,
              help='output columns in tab-delimited format')
@click.argument('job_ids', nargs=-1)
def jobstats(all, debug, recent, threshold, melt, json, parse, job_ids):
    from bmetrica.jobstats import JobStats
    try:
        js = JobStats(
            all=all,
            debug=debug,
            recent=recent,
            threshold=threshold,
            melt=melt,
            json=json,
            parse=parse
        )
        data = js.get_metrics(job_ids)
        js.display(data)
    except Exception, err:
        msg = '[err]: {}'.format(str(err))
        sys.exit(msg)

@cli.command(short_help="List recent jobs run for a given hostgroup")
@click.option('--start', '-s', default=default_start_time_threshold,
              show_default=True, type=click.STRING,
              help=("only look for jobs after this timestamp "
                    "threshold in 'YYYY-MM-DD H:M:S' format"))
@click.option('--end', '-e', default=default_end_time_threshold,
              show_default=True, type=click.STRING,
              help=("only look for jobs before a timestamp "
                    "threshold in 'YYYY-MM-DD H:M:S' format"))
@click.option('--users', '-u', is_flag=True, default=False,
              help='show users summary report')
@click.option('--hosts', '-k', is_flag=True, default=False,
              help='show hosts summary report')
@click.option('--debug', '-d', is_flag=True, default=False,
              help='show SQL query used')
@click.option('--parse', '-p', is_flag=True, default=False,
              help='output columns in tab-delimited format')
@click.argument('hostgroup', nargs=-1)
def hostgroups(start, end, users, hosts, debug, parse, hostgroup):
    from bmetrica.hostgroup import HGStats
    try:
        hg = HGStats(
            start=start,
            end=end,
            users=users,
            hosts=hosts,
            debug=debug,
            parse=parse,
        )
        data = hg.get_metrics(hostgroup)
        hg.display(data)
    except Exception, err:
        msg = '[err]: {}'.format(str(err))
        sys.exit(msg)
