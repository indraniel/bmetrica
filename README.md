# `bmetrica` _(python-edition)_

A CLI interface to IBM's [Spectrum LSF][0] (formerly [Platform LSF][1]) backend [MySQL][3]/[Cacti][4] database.

# Installation

    pip install git+https://github.com/indraniel/bmetrica.git

# Development

    git clone https://github.com/indraniel/bmetrica.git
    cd bmetrica
    virtualenv venv
    source venv/bin/activate
    pip install --no-cache-dir --process-dependency-links -e .
    <do development work in this directory>

    # clean up development workspace
    make clean

# Usage

You'll need to set the location of the MySQL/Cacti database resource via a shell environment variable, called `BMETRICA_DSN`, specified in a [RFC-1738][4] formatted URL.  For example in bash:

    export BMETRICA_DSN="mysql://<user>:<password>@<host>:<port>/<database>"
    
In most LSF/Cacti installations the default `<database>` name is usually called `cacti`.  Once `BMETRICA_DSN` is set, you can proceed to use the `bmetrica` command, or python API.

## Command Line

`bmetrica` uses a git-like subcommand interface.  Currently, only the `jobstats` subcommand is available.

    bmetrica --help
    bmetrica jobstats --help

    bmetrica jobstats 1234 5678
    bmetrica jobstats --parse 1234 5678
    bmetrica jobstats --melt 1234 5678
    bmetrica jobstats --melt --parse 1234 5678
    bmetrica jobstats --json 1234 5678

    cat list-of-lsf-job-ids-separated-by-newlines.txt | bmetrica jobstats -
    bmetrica jobstats - < cat list-of-lsf-job-ids-separated-by-newlines.txt 

## Python API

```python
from bmetrica.jobstats import JobStats

js = JobStats()
metrics = js.get_metrics([1234])

metrics
# [{u'command': "echo 'hello'",
#   u'cpu_used': 0.016,
#   u'cwd': '$HOME',
#   u'efficiency': Decimal('0.00000'),
#   u'end_time': datetime.datetime(2017, 3, 12, 13, 15, 49),
#   u'errFile': '/dev/null',
#   u'execCwd': '/home/me',
#   u'execUsername': 'me',
#   u'exec_host': 'bigbox123.acme.com',
#   u'from_host': 'little-box-123.acme.com',
#   u'jobName': "echo 'hello'",
#   u'jobPid': 7890,
#   u'jobid': 1234,
#   u'last_updated': datetime.datetime(2017, 3, 12, 13, 28, 41),
#   u'max_memory': 0.0,
#   u'max_swap': 0.0,
#   u'mem_requested': 0.0,
#   u'mem_reserved': 0.0,
#   u'mem_used': 0.0,
#   u'numPIDS': 0,
#   u'numThreads': 0,
#   u'num_cpus': 1,
#   u'num_nodes': 1,
#   u'outFile': '/dev/null',
#   u'projectName': 'default',
#   u'queue': 'short',
#   u'res_requirements': '',
#   u'rlimit_max_rss': 4000000,
#   u'run_time': 0,
#   u'start_time': datetime.datetime(2017, 3, 12, 13, 15, 49),
#   u'stat': 'DONE',
#   u'stime': 0.0,
#   u'submit_time': datetime.datetime(2017, 3, 12, 13, 15, 47),
#   u'user': 'me',
#   u'utime': 0.0}]
```

# LICENSE

Apache

[0]: http://www-03.ibm.com/systems/spectrum-computing/products/lsf/
[1]: https://en.wikipedia.org/wiki/Platform_LSF
[2]: https://en.wikipedia.org/wiki/MySQL
[3]: http://www.cacti.net/
[4]: https://www.ietf.org/rfc/rfc1738.txt
