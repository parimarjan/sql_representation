import networkx as nx
from .utils import *
import time
import itertools
import json
import pdb
from progressbar import progressbar as bar

def parse_sql(sql, user, db_name, db_host, port, pwd, timeout=120000, compute_ground_truth=True):
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

    print("query has",
          len(join_graph.nodes), "relations,",
          len(join_graph.edges), "joins, and",
          len(subset_graph), " possible subsets.",
          "took:", time.time() - start)

    ret = {}
    ret["sql"] = sql
    ret["join_graph"] = join_graph
    ret["subset_graph"] = subset_graph

    # we should check and see which cardinalities of the subset graph
    # we already know. Note thate we have to cache at this level because
    # the maximal matching might make arbitrary choices each time.
    with shelve.open("subset_cache") as cache:
        if sql in cache:
            currently_stored = cache[sql]
        else:
            currently_stored = {}

    unknown_subsets = subset_graph.copy()
    unknown_subsets = unknown_subsets.subgraph(subset_graph.nodes - currently_stored.keys())

    print(len(unknown_subsets.nodes), "/", len(subset_graph.nodes), "subsets still unknown")
    print(len(currently_stored), "subsets known.")
    if not compute_ground_truth:
        # TODO: convert these to json strs
        return ret

    # let us update the ground truth values
    edges = get_optimal_edges(unknown_subsets)
    paths = list(reconstruct_paths(edges))
    for p in paths:
        for el1, el2 in zip(p, p[1:]):
            assert len(el1) > len(el2)
        


    # ensure the paths we constructed cover every possible path
    sanity_check_unknown_subsets = unknown_subsets.copy()
    for n1, n2 in edges.items():
        if n1 in sanity_check_unknown_subsets.nodes:
            sanity_check_unknown_subsets.remove_node(n1)
        if n2 in sanity_check_unknown_subsets.nodes:
            sanity_check_unknown_subsets.remove_node(n2)

    assert len(sanity_check_unknown_subsets.nodes) == 0
    
    subset_sqls = []

    for path in paths:
        join_order = [tuple(sorted(x)) for x in path_to_join_order(path)]
        join_order.reverse()
        sql_to_exec = _nodes_to_sql(join_order, join_graph)
        sql_to_exec = "explain (analyze on, timing off, format json) " + sql_to_exec
        subset_sqls.append(sql_to_exec)
              

    print("computing all", len(unknown_subsets), "unknown subset cardinalities with"
          , len(subset_sqls), "queries")

    # TODO: parallelize this
    pre_exec_sqls = []

    # TODO: if we use the min #queries approach, maybe greedy approach and
    # letting pg choose join order is better?
    pre_exec_sqls.append("set join_collapse_limit to 1")
    pre_exec_sqls.append("set from_collapse_limit to 1")
    if timeout:
        pre_exec_sqls.append("set statement_timeout = {}".format(timeout))

    sanity_check_unknown_subsets = unknown_subsets.copy()
    for path_sql in bar(subset_sqls):
        print(path_sql)
        res = execute_query(path_sql, user, db_host, port, pwd, db_name,
                                        pre_exec_sqls)
        plan = res[0][0][0]
        plan_tree = plan["Plan"]
        results = list(analyze_plan(plan_tree))
        for result in results:
            assert nx.is_connected(join_graph.subgraph(result["aliases"])), (result["aliases"], plan_tree)
            aliases_key = tuple(sorted(result["aliases"]))
            currently_stored[aliases_key] = {"expected": result["expected"],
                                             "actual": result["actual"]}
            
            if aliases_key in sanity_check_unknown_subsets.nodes:
                sanity_check_unknown_subsets.remove_node(aliases_key)

    print(len(currently_stored), "total subsets now known")

    print("Still unknown:", sanity_check_unknown_subsets.nodes)
    
    with shelve.open("subset_cache") as cache:
        cache[sql] = currently_stored

    print("total time:", time.time() - start)

    return ret

def _nodes_to_sql(nodes, join_graph):
    alias_mapping = {}
    for node_set in nodes:
        for node in node_set:
            alias_mapping[node] = join_graph.nodes[node]["real_name"]

    from_clause = order_to_from_clause(join_graph, nodes, alias_mapping)

    subg = join_graph.subgraph(alias_mapping.keys())
    assert nx.is_connected(subg)
    
    sql_str = nx_graph_to_query(subg, from_clause=from_clause)
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
