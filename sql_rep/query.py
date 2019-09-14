import networkx as nx
from .utils import *
import time
import itertools
import json
import pdb

def parse_sql(sql, user, db_name, db_host, port, pwd,
     sql_cache_dir=None, timeout=120000,
        compute_ground_truth=True):
    '''
    @sql: sql query string.

    @ret: python dict with the keys:
        sql: original sql string
        join_graph: networkX graph representing query and its
        join_edges. Properties include:
            Nodes:
                - table
                - alias
                # FIXME: matches, or separate it out into ops AND predicates
                - matches
            Edges:
                - join_condition

            Note: This is the only place where these strings will be stored.
            Each of the subqueries will be represented by their nodes within
            the join_graph, and we can use these properties to reconstruct the
            appropriate query for the subsets.

        subset_graph: networkX graph representing each subquery.
        Properties include all the ground truth data that will need to be
        computed:
            - true_count
            - pg_count
            - total_count
    '''
    if compute_ground_truth:
        # all the appropriate db keywords must be set
        assert user is not None

    start = time.time()
    join_graph = _extract_join_graph(sql)
    subset_graph = generate_subset_graph(join_graph)

    print("num subqueries: ", len(subset_graph))
    print("took: ", time.time() - start)

    ret = {}
    ret["sql"] = sql
    ret["join_graph"] = join_graph
    ret["subset_graph"] = subset_graph

    if not compute_ground_truth:
        # TODO: convert these to json strs
        return ret

    # execution time, to collect ground truth

    # let us update the ground truth values
    edges = get_optimal_edges(subset_graph)
    paths = list(reconstruct_paths(edges))
    print("edges: ", len(edges))
    print("paths: ", len(paths))

    subset_sqls = []

    for path in paths:
        print(path)
        join_order = list(path_to_join_order(path))
        join_order.reverse()
        join_order_nodes = []

        # FIXME: can we choose the join order of each set more intelligently in
        # order to prevent bad plans slowing everything down?
        for j_set in join_order:
            # within a set, the order doesn't matter
            for j_node in j_set:
                join_order_nodes.append(j_node)
        print(join_order_nodes)
        sql = _nodes_to_sql(join_order_nodes, join_graph)
        sql = "explain (analyze on, timing off, format json) " + sql
        print(sql)
        subset_sqls.append(sql)

    # TODO: parallelize this
    pre_exec_sqls = []

    # TODO: if we use the min #queries approach, maybe greedy approach and
    # letting pg choose join order is better?
    pre_exec_sqls.append("set join_collapse_limit to 1")
    pre_exec_sqls.append("set from_collapse_limit to 1")
    if timeout:
        pre_exec_sqls.append("set statement_timeout = {}".format(timeout))

    sql = subset_sqls[20]
    res = cached_execute_query(sql, user, db_host, port, pwd, db_name,
            pre_exec_sqls,
            30, sql_cache_dir)

    plan = res[0][0][0]
    plan_tree = plan["Plan"]
    results = list(analyze_plan(plan_tree))
    for result in results:
        assert nx.is_connected(join_graph.subgraph(result["aliases"]))
        print(result)

    return ret

def _nodes_to_sql(nodes, join_graph):
    subg = join_graph.subgraph(nodes)
    assert nx.is_connected(subg)
    sql_str = nx_graph_to_query(subg)
    return sql_str

def _extract_join_graph(sql):
    '''
    @sql: string
    '''
    froms,aliases,tables = extract_from_clause(sql)
    joins = extract_join_clause(sql)
    join_graph = nx.Graph()

    for j in joins:
        j1 = j.split("=")[0]
        j2 = j.split("=")[1]
        t1 = j1[0:j1.find(".")].strip()
        t2 = j2[0:j2.find(".")].strip()
        try:
            assert t1 in tables or t1 in aliases
            assert t2 in tables or t2 in aliases
        except:
            print(t1, t2)
            print(tables)
            print(joins)
            print("table not in tables!")
            pdb.set_trace()

        join_graph.add_edge(t1, t2)
        join_graph[t1][t2]["join_condition"] = j
        if t1 in aliases:
            table1 = aliases[t1]
            table2 = aliases[t2]

            join_graph.nodes()[t1]["real_name"] = table1
            join_graph.nodes()[t2]["real_name"] = table2

    parsed = sqlparse.parse(sql)[0]
    # let us go over all the where clauses
    where_clauses = None
    for token in parsed.tokens:
        if (type(token) == sqlparse.sql.Where):
            where_clauses = token
    assert where_clauses is not None

    for t1 in join_graph.nodes():
        tables = [t1]
        matches = find_all_clauses(tables, where_clauses)
        join_graph.nodes()[t1]["predicates"] = matches

    return join_graph
