import networkx as nx
from .utils import *
import time
import itertools
import json
import pdb
from progressbar import progressbar as bar

def get_subset_cache_name(sql):
    return str(deterministic_hash(sql)[0:5])

def parse_sql(sql, user, db_name, db_host, port, pwd, timeout=False,
        compute_ground_truth=True, subset_cache_dir="./subset_cache/"):
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
    join_graph = extract_join_graph(sql)
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

    make_dir(subset_cache_dir)
    subset_cache_file = subset_cache_dir + get_subset_cache_name(sql)
    # we should check and see which cardinalities of the subset graph
    # we already know. Note thate we have to cache at this level because
    # the maximal matching might make arbitrary choices each time.
    with shelve.open(subset_cache_file) as cache:
        if sql in cache:
            currently_stored = cache[sql]
        else:
            currently_stored = {}

    unknown_subsets = subset_graph.copy()
    unknown_subsets = unknown_subsets.subgraph(subset_graph.nodes - currently_stored.keys())

    print(len(unknown_subsets.nodes), "/", len(subset_graph.nodes), "subsets still unknown (",
          len(currently_stored), "known )")


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
        sql_to_exec = nodes_to_sql(join_order, join_graph)
        if compute_ground_truth:
            prefix = "explain (analyze, timing off, format json) "
        else:
            prefix = "explain (analyze off, timing off, format json) "
        sql_to_exec = prefix + sql_to_exec
        subset_sqls.append(sql_to_exec)

    print("computing all", len(unknown_subsets), "unknown subset cardinalities with"
          , len(subset_sqls), "queries")

    pre_exec_sqls = []

    # TODO: if we use the min #queries approach, maybe greedy approach and
    # letting pg choose join order is better?
    pre_exec_sqls.append("set join_collapse_limit to 1")
    pre_exec_sqls.append("set from_collapse_limit to 1")
    if timeout:
        pre_exec_sqls.append("set statement_timeout = {}".format(timeout))

    sanity_check_unknown_subsets = unknown_subsets.copy()
    for idx, path_sql in enumerate(bar(subset_sqls)):
        res = execute_query(path_sql, user, db_host, port, pwd, db_name,
                            pre_exec_sqls)
        if res is None:
            print("Query failed to execute, ignoring.")
            breakpoint()
            continue

        plan = res[0][0][0]
        plan_tree = plan["Plan"]
        results = list(analyze_plan(plan_tree))
        for result in results:
            # this assertion is invalid because PG may choose to use an implicit join predicate,
            # for example, if a.c1 = b.c1 and b.c1 = c.c1, then PG may choose to join on a.c1 = c.c1
            # assert nx.is_connected(join_graph.subgraph(result["aliases"])), (result["aliases"], plan_tree)
            aliases_key = tuple(sorted(result["aliases"]))
            if compute_ground_truth:
                currently_stored[aliases_key] = {"expected": result["expected"],
                                                 "actual": result["actual"]}
            else:
                currently_stored[aliases_key] = {"expected": result["expected"]}

            if aliases_key in sanity_check_unknown_subsets.nodes:
                sanity_check_unknown_subsets.remove_node(aliases_key)

        if idx % 5 == 0:
            with shelve.open(subset_cache_file) as cache:
                cache[sql] = currently_stored

    print(len(currently_stored), "total subsets now known")

    assert len(sanity_check_unknown_subsets.nodes) == 0

    with shelve.open(subset_cache_file) as cache:
        cache[sql] = currently_stored

    for node in subset_graph.nodes:
        subset_graph.nodes[node]["cardinality"] = currently_stored[node]

    print("total time:", time.time() - start)

    # json-ify the graphs
    ret["join_graph"] = nx.adjacency_data(ret["join_graph"])
    ret["subset_graph"] = nx.adjacency_data(ret["subset_graph"])

    return ret

