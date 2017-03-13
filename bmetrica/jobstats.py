from __future__ import print_function

import sys, os, re, json

import pymysql.cursors
from jinja2 import Template

class JobStats(object):
    def __init__(self, all=False, debug=False, recent=False,
                       melt=False, json=False, parse=False,
                       threshold='1970-01-01 00:00:00'):
        self.check_environment_variables()
        self.all = all
        self.debug = debug
        self.recent = recent
        self.threshold = threshold
        self.melt = melt
        self.json = json
        self.parse = parse
        self.connection = self.connect_db()
        self.metrics = [
            "jobid",
            "user",
            "execUsername",
            "command",
            "execCwd",
            "cwd",
            "cpu_used",
            "efficiency",
            "submit_time",
            "start_time",
            "end_time",
            "stat",
            "from_host",
            "exec_host",
            "jobPid",
            "last_updated",
            "mem_requested",
            "mem_reserved",
            "mem_used",
            "max_memory",
            "max_swap",
            "numPIDS",
            "numThreads",
            "num_nodes",
            "num_cpus",
            "res_requirements",
            "rlimit_max_rss",
            "run_time",
            "stime",
            "utime",
            "queue",
            "errFile",
            "outFile",
            "projectName",
            "jobName",
        ]
        self.floating_point_columns = set([
            "cpu_used",
            "efficiency",
            "mem_requested",
            "mem_reserved",
            "mem_used",
            "max_memory",
            "max_swap",
            "run_time",
            "stime",
            "utime",
        ])
        self.nullable_columns = set([
            "exec_host",
            "mem_used",
            "projectName",
            "jobName",
        ])
        self.datetime_columns = set([
            "submit_time",
            "start_time",
            "end_time",
            "last_updated",
        ])

    def connect_db(self):
        dsn = os.environ['BMETRICA_DSN']
        (host, user, password, port, database) = self.parse_dsn(dsn)
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=database,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return connection

    def parse_dsn(self, dsn):
        regexp = ('(?P<driver>\w+)://'
                  '(?P<user>\w+):'
                  '(?P<password>\w+)@(?P<host>\S+):'
                  '(?P<port>\d+)/(?P<database>\w+)')

        if self.debug:
            print('dsn is: {}'.format(dsn), file=sys.stderr)

        m = re.search(regexp, dsn)
        data = m.groupdict()

        needed_elems = ('host', 'user', 'password', 'port', 'database')

        for e in needed_elems:
            if (not e in data) or (not data[e]):
                msg = "Did not find '{}' in dsn: '{}'".format(e, dsn)
                raise RuntimeError(msg)

        data['port'] = int(data['port'])

        return [ data[e] for e in needed_elems ]

    def check_environment_variables(self):
        if 'BMETRICA_DSN' not in os.environ:
            msg = ("Please set the 'BMETRICA_DSN' "
                   "shell environment variable!" )
            raise RuntimeError(msg)

    def get_metrics(self, job_ids):
        stats = []
        for id in job_ids:
            if id.isdigit():
                metrics = self.query(int(id))
                if metrics: stats.extend(metrics)
            elif id == '-':
                for line in sys.stdin:
                    item = line.rstrip()
                    if item.isdigit():
                        metrics = self.query(int(item))
                        if metrics: stats.extend(metrics)
                    else:
                        print("[WARN] SKIPPING '{}' -- NOT LSF ID".format(id))
            else:
                print("[WARN] SKIPPING '{}' -- NOT LSF ID".format(id))
        return stats

    def query(self, job_id):
        tables = self.get_grid_job_tables()
        sql = self.construct_metrics_sql(tables)
        metrics = self.execute_metrics_sql(tables, sql, job_id)
        return metrics

    def display(self, stats):
        if not stats:
            print("Found no LSF job stats", file=sys.stderr)
            sys.exit(0)

        if self.melt:
            self.display_melt(stats)
        elif self.json:
            self.display_json(stats)
        elif self.parse:
            self.display_parse(stats)
        else:
            self.display_standard(stats)

    def display_melt(self, stats):
        fmt_widths = ("{:<10}", "{:<20}", "{:<20}", "{}")
        headers = ('JobId', 'SubmitTime', 'Field', 'Attribute')
        separators = [ "=" * i for i in (10, 20, 20, 30) ]
        attrs = [ col for col in self.metrics
                      if col not in ('jobid', 'submit_time') ]

        if self.parse:
            fmt = "\t".join(fmt_widths)
        else:
            fmt = "  ".join(fmt_widths)

        print(fmt.format(*headers))
        print(fmt.format(*separators))
        sys.stdout.flush()

        data = self._render_formatted_strings(stats)
        for row in data:
            (jobid, submit_time) = (row["jobid"], row["submit_time"])
            for attr in attrs:
                print(fmt.format(jobid, submit_time, attr, row[attr]))
        sys.stdout.flush()


    def display_json(self, stats):
        data = self._render_formatted_strings(stats)
        print(json.dumps(data,
                         sort_keys=True,
                         indent=4,
                         separators=(', ', ': ')))

    def display_parse(self, stats):
        data = self._render_formatted_strings(stats)
        print("\t".join(self.metrics))
        sys.stdout.flush()
        for row in data:
            elems = [ row[col] for col in self.metrics ]
            print("\t".join(elems))
        sys.stdout.flush()

    def display_standard(self, stats):
        data = self._render_formatted_strings(stats)
        fmts = self._derived_standard_fmt_widths(data)
        fmt = "\t".join(fmts)
        print(fmt.format(*self.metrics))
        sys.stdout.flush()
        for row in data:
            elems = [ row[col] for col in self.metrics ]
            print(fmt.format(*elems))
            sys.stdout.flush()

    def _render_formatted_strings(self, stats):
        rendered = [ self._generate_std_row_strings(row) for row in stats ]
        return rendered

    def _generate_std_row_strings(self, row):
        rendered = {}
        for col in self.metrics:
            if col in self.floating_point_columns:
                rendered[col] = "{:g}".format(row[col])
            elif col in self.datetime_columns:
                rendered[col] = "{:%Y-%m-%d %T}".format(row[col])
            else:
                rendered[col] = "{}".format(str(row[col]))
        return rendered

    def _derived_standard_fmt_widths(self, stats):
        # widths based on the column header texts
        widths = { col : len(col) for col in self.metrics }

        # widths based on the metrics
        for col in self.metrics:
            max_length = max([ len(str(row[col])) for row in stats ])
            widths[col] = max(max_length, widths[col])

        fmts = [ "{}:<{}{}".format('{', widths[i], '}') for i in self.metrics ]
        return fmts

    def get_grid_job_tables(self):
        # these are the latest tables that aren't formally sharded
        tables = ['grid_jobs', 'grid_jobs_finished']
        if self.recent:
            return tables

        sql = '''
            select Concat(table_name, '_v', partition)
            from grid_table_partitions
            where table_name = "grid_jobs_finished"
            and
            (min_time > %s or max_time > %s)
            order by partition desc
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (self.threshold, self.threshold))
            result = cursor.fetchall()
        sharded_tables = [ i.values()[0] for i in result ]
        tables.extend(sharded_tables)
        return tables

    def construct_metrics_sql(self, grid_job_tables):
        columns = [ "`{}`".format(i) for i in self.metrics ]
        sql = '''
            SELECT {{ columns | join(', ') }}
            FROM (
            {% for table in tables -%}
                (select {{ columns | join(', ') }}
                 from   {{ table }}
                 where  jobid = %s)
                {% if not table == tables | last -%}
                UNION
                {% endif %}
            {%- endfor -%}
            ) AS merged
            where merged.submit_time >= %s
            order by DATE(merged.submit_time) DESC, DATE(merged.start_time) DESC
            {% if not showAll -%}
            LIMIT 1
            {%- endif -%}
        '''
        template = Template(sql)
        rendered_sql = template.render(
            tables=grid_job_tables,
            columns=columns,
            showAll=self.all
        )
        if self.debug: print(rendered_sql, file=sys.stderr)
        return rendered_sql

    def execute_metrics_sql(self, grid_job_tables, sql, job_id):
        params = [ job_id ] * len(grid_job_tables)
        params.append(self.threshold)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
        return results
