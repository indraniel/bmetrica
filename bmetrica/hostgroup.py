from __future__ import print_function

import sys, os, re, json
import datetime as dt

import pymysql.cursors
from jinja2 import Template


class HGStats(object):
    def __init__(self, start=None, end=None,
                       users=None, hosts=None,
                       detail=None,
                       debug=False, parse=False):
        self.check_environment_variables()

        if start is None:
            # 30 days ago
            self.start = (dt.datetime.now() - dt.timedelta(30))
        else:
            self.start = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        if end is None:
            # 30 days ago
            self.end = dt.datetime.now()
        else:
            self.end = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        self.detail = detail
        self.users = users
        self.hosts = hosts
        self.debug = debug
#        self.json = json
        self.parse = parse
        self.connection = self.connect_db()
        self.overall_summary_metrics = [
            "hostgroup",
            "status",
            "counts",
        ]
        self.users_summary_metrics = [
            "user",
            "status",
            "counts",
        ]
        self.hosts_summary_metrics = [
            "host",
            "status",
            "counts",
        ]
        self.detail_metrics = [
            "groupName",
            "jobid",
            "user",
            "command",
            "execCwd",
            "cwd",
            "submit_time",
            "start_time",
            "end_time",
            "stat",
            "from_host",
            "exec_host",
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
            "projectName",
            "jobName",
        ])
        self.datetime_columns = set([
            "submit_time",
            "start_time",
            "end_time",
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

    def get_metrics(self, hostgroup):
        if self.detail:
            raise RuntimeError("the detail option is not yet implemented!")

        if bool(hostgroup) is False:
            metrics = self.overall_hostgroup_summary_query()
        elif self.users:
            metrics = self.hostgroup_users_summary_query(hostgroup)
        elif self.hosts:
            metrics = self.hostgroup_hosts_summary_query(hostgroup)
        else:
            metrics = []
        return metrics

    def overall_hostgroup_summary_query(self):
        tables = self.get_grid_job_tables()
        sql = self.construct_overall_metrics_sql(tables)
        metrics = self.execute_overall_metrics_sql(tables, sql)
        return metrics

    def hostgroup_users_summary_query(self, hostgroup):
        tables = self.get_grid_job_tables()
        hg = hostgroup[0]
        sql = self.construct_hostgroup_users_metrics_sql(tables)
        metrics = self.execute_hostgroup_users_metrics_sql(tables, sql, hg)
        return metrics

    def hostgroup_hosts_summary_query(self, hostgroup):
        tables = self.get_grid_job_tables()
        hg = hostgroup[0]
        sql = self.construct_hostgroup_hosts_metrics_sql(tables)
        metrics = self.execute_hostgroup_hosts_metrics_sql(tables, sql, hg)
        return metrics

    def display(self, stats):
        if not stats:
            print("Found no LSF job stats", file=sys.stderr)
            sys.exit(0)

        elif self.parse:
            self.display_parse(stats)
        else:
            self.display_standard(stats)

    def get_columns(self):
        if not self.hosts or self.users:
            columns = self.overall_summary_metrics
        if self.hosts:
            columns = self.hosts_summary_metrics
        if self.users:
            columns = self.users_summary_metrics
        return columns

    def display_parse(self, stats):
        columns = self.get_columns()
        data = self._render_formatted_strings(stats, columns)
        print("\t".join(columns))
        sys.stdout.flush()
        for row in data:
            elems = [ row[col] for col in columns ]
            print("\t".join(elems))
        sys.stdout.flush()

    def display_standard(self, stats):
        columns = self.get_columns()
        data = self._render_formatted_strings(stats, columns)
        fmts = self._derived_standard_fmt_widths(data, columns)
        fmt = "\t".join(fmts)
        print(fmt.format(*columns))
        sys.stdout.flush()
        for row in data:
            elems = [ row[col] for col in columns ]
            print(fmt.format(*elems))
            sys.stdout.flush()

    def _render_formatted_strings(self, stats, columns):
        rendered = [ self._generate_std_row_strings(row, columns) for row in stats ]
        return rendered

    def _generate_std_row_strings(self, row, columns):
        rendered = {}
        for col in columns:
            if col in self.floating_point_columns:
                rendered[col] = "{:g}".format(row[col])
            elif col in self.datetime_columns:
                rendered[col] = "{:%Y-%m-%d %T}".format(row[col])
            else:
                rendered[col] = "{}".format(str(row[col]))
        return rendered

    def _derived_standard_fmt_widths(self, stats, columns):
        # widths based on the column header texts
        widths = { col : len(col) for col in columns }

        # widths based on the metrics
        for col in columns:
            max_length = max([ len(str(row[col])) for row in stats ])
            widths[col] = max(max_length, widths[col])

        fmts = [ "{}:<{}{}".format('{', widths[i], '}') for i in columns ]
        return fmts

    def get_grid_job_tables(self):
        # these are the latest tables that aren't formally sharded
        tables = [('grid_jobs', 'A'), ('grid_jobs_finished', 'B')]

        sql = '''
            select Concat(table_name, '_v', partition) fullname, partition
            from grid_table_partitions
            where table_name = "grid_jobs_finished"
            and
            (min_time > %s and max_time <= %s)
            order by partition desc
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (self.start.strftime("%Y-%m-%d %H:%M:%S"), self.end.strftime("%Y-%m-%d %H:%M:%S")))
            result = cursor.fetchall()
        sharded_tables = [ (i['fullname'], i['partition']) for i in result ]
        tables.extend(sharded_tables)
        return tables

    def construct_overall_metrics_sql(self, grid_job_tables):
        columns = [ "`{}`".format(i) for i in self.detail_metrics ]
        sql = '''
            SELECT groupName hostgroup, `stat` status, count(*) counts
            FROM (
            {% for table_name,table_abbrev in tables -%}
                (select {{ columns | join(', ') }}
                 from   {{ table_name }} v{{ table_abbrev }}
                 join   grid_hostgroups gh{{ table_abbrev }} on gh{{ table_abbrev }}.host = v{{ table_abbrev }}.exec_host
                 where  v{{ table_abbrev }}.submit_time > %s
                 and    v{{ table_abbrev }}.submit_time <= %s)
                {% if not loop.last -%}
                UNION
                {% endif %}
            {%- endfor -%}
            ) AS merged
            group by merged.groupName, merged.stat
            order by merged.groupName DESC, merged.stat DESC
        '''
        template = Template(sql)
        rendered_sql = template.render(
            tables=grid_job_tables,
            columns=columns,
        )
        if self.debug: print(rendered_sql, file=sys.stderr)
        return rendered_sql

    def execute_overall_metrics_sql(self, grid_job_tables, sql):
        start_ts = self.start.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = self.end.strftime("%Y-%m-%d %H:%M:%S")
        params = [ start_ts , end_ts ] * len(grid_job_tables)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
        return results

    def construct_hostgroup_users_metrics_sql(self, grid_job_tables):
        columns = [ "`{}`".format(i) for i in self.detail_metrics ]
        sql = '''
            SELECT user, `stat` status, count(*) counts
            FROM (
            {% for table_name,table_abbrev in tables -%}
                (select {{ columns | join(', ') }}
                 from   {{ table_name }} v{{ table_abbrev }}
                 join   grid_hostgroups gh{{ table_abbrev }} on gh{{ table_abbrev }}.host = v{{ table_abbrev }}.exec_host
                 where  v{{ table_abbrev }}.submit_time > %s
                 and    v{{ table_abbrev }}.submit_time <= %s
                 and    gh{{ table_abbrev }}.groupName = %s)
                {% if not loop.last -%}
                UNION
                {% endif %}
            {%- endfor -%}
            ) AS merged
            group by merged.user, merged.stat
            order by merged.user DESC, merged.stat DESC
        '''
        template = Template(sql)
        rendered_sql = template.render(
            tables=grid_job_tables,
            columns=columns,
        )
        if self.debug: print(rendered_sql, file=sys.stderr)
        return rendered_sql

    def execute_hostgroup_users_metrics_sql(self, grid_job_tables, sql, hostgroup):
        start_ts = self.start.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = self.end.strftime("%Y-%m-%d %H:%M:%S")
        params = [ start_ts , end_ts, hostgroup ] * len(grid_job_tables)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
        return results

    def construct_hostgroup_hosts_metrics_sql(self, grid_job_tables):
        columns = [ "`{}`".format(i) for i in self.detail_metrics ]
        sql = '''
            SELECT exec_host host, `stat` status, count(*) counts
            FROM (
            {% for table_name,table_abbrev in tables -%}
                (select {{ columns | join(', ') }}
                 from   {{ table_name }} v{{ table_abbrev }}
                 join   grid_hostgroups gh{{ table_abbrev }} on gh{{ table_abbrev }}.host = v{{ table_abbrev }}.exec_host
                 where  v{{ table_abbrev }}.submit_time > %s
                 and    v{{ table_abbrev }}.submit_time <= %s
                 and    gh{{ table_abbrev }}.groupName = %s)
                {% if not loop.last -%}
                UNION
                {% endif %}
            {%- endfor -%}
            ) AS merged
            group by merged.exec_host, merged.stat
            order by merged.exec_host DESC, merged.stat DESC
        '''
        template = Template(sql)
        rendered_sql = template.render(
            tables=grid_job_tables,
            columns=columns,
        )
        if self.debug: print(rendered_sql, file=sys.stderr)
        return rendered_sql

    def execute_hostgroup_hosts_metrics_sql(self, grid_job_tables, sql, hostgroup):
        start_ts = self.start.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = self.end.strftime("%Y-%m-%d %H:%M:%S")
        params = [ start_ts , end_ts, hostgroup ] * len(grid_job_tables)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
        return results
