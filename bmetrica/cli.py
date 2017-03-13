from __future__ import print_function, division
import signal, sys

import click

from .version import __version__

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
def cli():
    '''bmetrica: A CLI interface to IBM's Plaform LSF backend MySQL/Cacti database.'''
    # to make this script/module behave nicely with unix pipes
    # http://newbebweb.blogspot.com/2012/02/python-head-ioerror-errno-32-broken.html
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

@cli.command(short_help="Job statistics on submitted LSF Job ID(s)")
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
