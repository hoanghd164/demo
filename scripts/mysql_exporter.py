import mysql.connector
import sys
import os
from mysql.connector import Error

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import (
    load_config,
    get_str,
    write_prometheus_metrics
)

def db_querry(query=None):
    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_passwd,
            database=db_name,
            connect_timeout=2
        )
        if query is not None:
            cursor = connection.cursor()
            cursor.execute(query)
            db_results = cursor.fetchall()
            return db_results

        if connection.is_connected():
            if query is not None:
                cursor.close()
            connection.close()

    except Error as e:
        raise Exception(f"Error: {str(e)}")

def db_access_stats():
    list_db_access = []
    current_connections = {
        "name": "mysql_current_connections",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of current connections",
        "value": float(db_querry("SHOW STATUS LIKE 'Threads_connected';")[0][1])
    }

    max_connections = {
        "name": "mysql_max_connections",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Maximum number of connections that can be made",
        "value": float(db_querry("SHOW VARIABLES LIKE 'max_connections';")[0][1])
    }

    uptime = {
        "name": "mysql_uptime",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of seconds the server has been running",
        "value": float(db_querry("SHOW STATUS LIKE 'Uptime';")[0][1])
    }

    queries_per_second = {
        "name": "mysql_queries",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of queries that have been executed",
        "value": float(db_querry("SHOW STATUS LIKE 'Queries';")[0][1])
    }

    slow_queries = {
        "name": "mysql_slow_queries",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of slow queries that have been executed",
        "value": float(db_querry("SHOW STATUS LIKE 'Slow_queries';")[0][1])
    }

    current_queries = {
        "name": "mysql_threads_running",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of threads that are currently running",
        "value": float(db_querry("SHOW STATUS LIKE 'Threads_running';")[0][1])
    }

    table_locks_waited = {
        "name": "mysql_table_locks_waited",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of table locks that have been waited for",
        "value": float(db_querry("SHOW STATUS LIKE 'Table_locks_waited';")[0][1])
    }

    open_tables = {
        "name": "mysql_open_tables",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of tables that are currently open",
        "value": float(db_querry("SHOW STATUS LIKE 'Open_tables';")[0][1])
    }

    active_threads = {
        "name": "mysql_active_threads",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of active threads",
        "value": len(db_querry("SHOW PROCESSLIST;"))
    }

    bytes_sent = {
        "name": "mysql_bytes_sent",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of bytes that have been sent",
        "value": float(db_querry("SHOW STATUS LIKE 'Bytes_sent';")[0][1])
    }

    bytes_received = {
        "name": "mysql_bytes_received",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of bytes that have been received",
        "value": float(db_querry("SHOW STATUS LIKE 'Bytes_received';")[0][1])
    }

    aborted_connections = {
        "name": "mysql_aborted_connects",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of connections that have been aborted",
        "value": float(db_querry("SHOW STATUS LIKE 'Aborted_connects';")[0][1])
    }
        
    mysql_select_types = {
        "name": "mysql_select_types",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of select types that have been done",
        "value": float(db_querry("SHOW STATUS LIKE 'Select_full_join';")[0][1])
    }

    mysql_temporary_objects = {
        "name": "mysql_temporary_objects",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of temporary objects that have been created",
        "value": float(db_querry("SHOW STATUS LIKE 'Created_tmp_tables';")[0][1])
    }

    mysql_sorts = {
        "name": "mysql_sorts",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of sorts that have been done",
        "value": float(db_querry("SHOW STATUS LIKE 'Sort_merge_passes';")[0][1])
    }

    command_counters = {
        "name": "mysql_command_counters",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of times a command has been executed",
        "value": float(db_querry("SHOW STATUS LIKE 'Com_select';")[0][1])
    }

    mysql_handler = {
        "name": "mysql_handler",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of times the handler has been called",
        "value": float(db_querry("SHOW STATUS LIKE 'Handler_read_rnd_next';")[0][1])
    }

    mysql_query_cache_memory = {
        "name": "mysql_query_cache_memory",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Amount of memory used by the query cache",
        "value": float(db_querry("SHOW STATUS LIKE 'Qcache_free_memory';")[0][1])
    }

    mysql_query_cache_activity = {
        "name": "mysql_query_cache_activity",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of queries that have been cached",
        "value": float(db_querry("SHOW STATUS LIKE 'Qcache_hits';")[0][1])
    }

    mysql_file_openings = {
        "name": "mysql_file_openings",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of files that have been opened",
        "value": float(db_querry("SHOW STATUS LIKE 'Opened_files';")[0][1])
    }

    mysql_table_openings = {
        "name": "mysql_table_openings",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of tables that have been opened",
        "value": float(db_querry("SHOW STATUS LIKE 'Opened_tables';")[0][1])
    }

    mysql_open_files = {
        "name": "mysql_open_files",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of files that are currently open",
        "value": float(db_querry("SHOW STATUS LIKE 'Open_files';")[0][1])
    }

    mysql_open_tables = {
        "name": "mysql_open_tables",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of tables that are currently open",
        "value": float(db_querry("SHOW STATUS LIKE 'Open_tables';")[0][1])
    }

    mysql_version = {
        "name": "mysql_version",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Version of the MySQL server",
        "version": db_querry("SELECT VERSION();")[0][0],
        "value": 1
    }

    num_databases = {
        "name": "mysql_number_of_databases",
        "role": "access_stats",
        "db_host": db_host,
        "db_port": str(db_port),
        "description": "Number of databases",
        "value": float(db_querry("SELECT COUNT(*) FROM information_schema.schemata;")[0][0])
    }

    list_db_access.append(current_connections)
    list_db_access.append(max_connections)
    list_db_access.append(uptime)
    list_db_access.append(queries_per_second)
    list_db_access.append(slow_queries)
    list_db_access.append(current_queries)
    list_db_access.append(table_locks_waited)
    list_db_access.append(open_tables)
    list_db_access.append(active_threads)
    list_db_access.append(bytes_sent)
    list_db_access.append(bytes_received)
    list_db_access.append(aborted_connections)
    list_db_access.append(mysql_select_types)
    list_db_access.append(mysql_temporary_objects)
    list_db_access.append(mysql_sorts)
    list_db_access.append(command_counters)
    list_db_access.append(mysql_handler)
    list_db_access.append(mysql_query_cache_memory)
    list_db_access.append(mysql_query_cache_activity)
    list_db_access.append(mysql_file_openings)
    list_db_access.append(mysql_table_openings)
    list_db_access.append(mysql_open_files)
    list_db_access.append(mysql_open_tables)
    list_db_access.append(mysql_version)
    list_db_access.append(num_databases)

    return list_db_access

def db_cluster_stats():
    list_db_access = []
    list_db_access.append({
            "name": "wsrep_cluster_size",
            "role": "cluster_stats",
            "db_name": db_name,
            "db_host": db_host,
            "db_port": db_port,
            "description": "Number of nodes in the cluster",
            "value": float(db_querry("SHOW STATUS LIKE 'wsrep_cluster_size';")[0][1])
        })
    
    list_db_access.append({
        "name": "wsrep_cluster_role",
        "role": "cluster_stats",
        "db_name": db_name,
        "db_host": db_host,
        "db_port": db_port,
        "description": "1 - Primary, 2 - Secondary",
        "value": 1 if db_querry("SHOW STATUS LIKE 'wsrep_cluster_status';")[0][1] == 'Primary' else 2
    })
    return list_db_access

if __name__ == '__main__':
    try:
        final_results = []
        final_errors = []
        sensor_name = 'mysql'

        project = os.environ.get("PROJECT", "staging")
        config = load_config(project)

        sensor_cfg = config.get("sensor", {}).get(sensor_name, {})
        if not sensor_cfg.get("enable", False):
            sys.exit(0)

        targets = sensor_cfg.get("targets", [])
        for target in targets:
            if not target.get("enable", False):
                continue

            try:
                db_host = target["db_host"]
                db_port = target["db_port"]
                db_user = target["db_user"]
                db_passwd = target["db_passwd"]
                db_name = target["db_name"]
                globals().update(locals())

                final_results.extend(db_access_stats())
                final_results.extend(db_cluster_stats())

            except Exception as e:
                final_errors.append({
                    "name": "mysql_error",
                    "db_host": target.get("db_host"),
                    "db_name": target.get("db_name"),
                    "message": str(e).replace('"', "'"),
                    "value": 1
                })

        final_results.extend(final_errors)

        prom_dirs = get_str("NODE_EXPORTER_PROM_DIR", "/var/lib/node_exporter/textfile_collector").split(":")
        write_prometheus_metrics(prom_dirs, final_results, sensor_name)

    except Exception as e:
        print(f"❌ {sensor_name} failed: {e}", file=sys.stderr)
        sys.exit(1)