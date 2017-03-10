from __future__ import print_function, division
import signal, sys, os

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

@cli.command(short_help="Job information on LSF Job ID(s)")
@click.option()
def jobstats():
    pass
