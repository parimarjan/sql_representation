import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from moz_sql_parser import parse
import time
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from networkx.algorithms import bipartite
import networkx as nx
import itertools
import hashlib
import psycopg2 as pg

# get rid of these
import getpass

ALIAS_FORMAT = "{TABLE} AS {ALIAS}"
RANGE_PREDS = ["gt", "gte", "lt", "lte"]
COUNT_SIZE_TEMPLATE = "SELECT COUNT(*) FROM {FROM_CLAUSE}"

'''
functions copied over from ryan's utils files
'''

def connected_subgraphs(g):
    for i in range(2, len(g)+1):
        for nodes_in_sg in itertools.combinations(g.nodes, i):
            sg = g.subgraph(nodes_in_sg)
            if nx.is_connected(sg):
                yield tuple(sorted(sg.nodes))

def generate_subset_graph(g):
    subset_graph = nx.DiGraph()
    for csg in connected_subgraphs(g):
        subset_graph.add_node(csg)

    for sg1, sg2 in itertools.combinations(subset_graph.nodes, 2):
        if len(sg1) + 1 == len(sg2) and set(sg1) < set(sg2):
            subset_graph.add_edge(sg2, sg1)
        elif len(sg2) + 1 == len(sg1) and set(sg2) < set(sg1):
            subset_graph.add_edge(sg1, sg2)

    return subset_graph

def get_optimal_edges(sg, draw_each=False):
    paths = {}
    orig_sg = sg
    sg = sg.copy()
    while len(sg.edges) != 0:
        # first, find the root(s) of the subgraph.
        roots = {n for n,d in sg.in_degree() if d == 0}
        # find everything within reach of 1
        reach_1 = set()
        for root in roots:
            reach_1.update(sg.neighbors(root))

        # build a bipartite graph and do the matching
        all_nodes = reach_1 | roots
        bipart_layer = sg.subgraph(all_nodes).to_undirected()
        assert(bipartite.is_bipartite(bipart_layer))
        matching = bipartite.hopcroft_karp_matching(bipart_layer, roots)
        matching = { k: v for k,v in matching.items() if k in roots}

        if draw_each:
            draw_graph(orig_sg,
                       highlight_nodes=bipart_layer.nodes,
                       bold_edges=matching.items())

        paths.update(matching)

        # remove the old roots
        sg.remove_nodes_from(roots)
    return paths

def reconstruct_paths(edges):
    g = nx.Graph()
    for pair in edges.items():
        g.add_nodes_from(pair)

    for v1, v2 in edges.items():
        g.add_edge(v1, v2)

    conn_comp = nx.algorithms.components.connected_components(g)
    paths = (sorted(x, key=len, reverse=True) for x in conn_comp)
    return paths

def greedy(subset_graph, plot=False):
    subset_graph = subset_graph.copy()

    while subset_graph:
        longest_path = nx.algorithms.dag.dag_longest_path(subset_graph)
        if plot:
            display(draw_graph(subset_graph, highlight_nodes=longest_path))
        subset_graph.remove_nodes_from(longest_path)
        yield longest_path

def path_to_join_order(path):
    remaining = set(path[0])
    for node in path[1:]:
        diff = remaining - set(node)
        yield diff
        remaining -= diff
    yield remaining

def path_to_from_clause(path, alias_mapping):
    join_order = list(path_to_join_order(path))
    join_order.reverse()

    clauses = []
    for rels in join_order:
        clause = " CROSS JOIN ".join([f"{alias_mapping[alias]} as {alias}" for alias in rels])
        if len(rels) > 1:
            clause = "( " + clause + ")"
        clauses.append(clause)

    return " CROSS JOIN ".join(clauses)

join_types = set(["Nested Loop", "Hash Join"])

def extract_aliases(plan):
    if "Alias" in plan:
        yield plan["Alias"]

    if "Plans" not in plan:
        return

    for subplan in plan["Plans"]:
        yield from extract_aliases(subplan)

def analyze_plan(plan):
    if plan["Node Type"] in join_types:
        aliases = extract_aliases(plan)
        yield {"expected": plan["Plan Rows"],
               "actual": plan["Actual Rows"],
               "aliases": list(sorted(aliases))}

    if "Plans" not in plan:
        return

    for subplan in plan["Plans"]:
        yield from analyze_plan(subplan)

'''
functions copied over from pari's util files
'''

def nx_graph_to_query(G):
    froms = []
    conds = []
    for nd in G.nodes(data=True):
        node = nd[0]
        data = nd[1]
        if "real_name" in data:
            froms.append(ALIAS_FORMAT.format(TABLE=data["real_name"],
                                             ALIAS=node))
        else:
            froms.append(node)

        for pred in data["predicates"]:
            conds.append(pred)

    for edge in G.edges(data=True):
        conds.append(edge[2]['join_condition'])

    # preserve order for caching
    froms.sort()
    conds.sort()
    from_clause = " , ".join(froms)
    if len(conds) > 0:
        wheres = ' AND '.join(conds)
        from_clause += " WHERE " + wheres
    count_query = COUNT_SIZE_TEMPLATE.format(FROM_CLAUSE=from_clause)
    return count_query

def extract_join_clause(query):
    '''
    FIXME: this can be optimized further / or made to handle more cases
    '''
    parsed = sqlparse.parse(query)[0]
    # let us go over all the where clauses
    start = time.time()
    where_clauses = None
    for token in parsed.tokens:
        if (type(token) == sqlparse.sql.Where):
            where_clauses = token
    if where_clauses is None:
        return []
    join_clauses = []

    froms, aliases, table_names = extract_from_clause(query)
    if len(aliases) > 0:
        tables = [k for k in aliases]
    else:
        tables = table_names
    matches = find_all_clauses(tables, where_clauses)
    for match in matches:
        if "=" not in match:
            continue
        match = match.replace(";", "")
        left, right = match.split("=")
        # ugh dumb hack
        if "." in right:
            # must be a join, so add it.
            join_clauses.append(left.strip() + " = " + right.strip())

    return join_clauses

def get_all_wheres(parsed_query):
    pred_vals = []
    if "where" not in parsed_query:
        pass
    elif "and" not in parsed_query["where"]:
        pred_vals = [parsed_query["where"]]
    else:
        pred_vals = parsed_query["where"]["and"]
    return pred_vals

# FIXME: get rid of this dependency
def extract_predicates(query):
    '''
    @ret:
        - column names with predicate conditions in WHERE.
        - predicate operator type (e.g., "in", "lte" etc.)
        - predicate value
    Note: join conditions don't count as predicate conditions.

    FIXME: temporary hack. For range queries, always returning key
    "lt", and vals for both the lower and upper bound
    '''
    def parse_column(pred, cur_pred_type):
        '''
        gets the name of the column, and whether column location is on the left
        (0) or right (1)
        '''
        for i, obj in enumerate(pred[cur_pred_type]):
            assert i <= 1
            if isinstance(obj, str) and "." in obj:
                # assert "." in obj
                column = obj
            elif isinstance(obj, dict):
                assert "literal" in obj
                val = obj["literal"]
                val_loc = i
            else:
                val = obj
                val_loc = i

        assert column is not None
        assert val is not None
        return column, val_loc, val

    def _parse_predicate(pred, pred_type):
        if pred_type == "eq":
            columns = pred[pred_type]
            if len(columns) <= 1:
                return None
            # FIXME: more robust handling?
            if "." in str(columns[1]):
                # should be a join, skip this.
                # Note: joins only happen in "eq" predicates
                return None
            predicate_types.append(pred_type)
            predicate_cols.append(columns[0])
            predicate_vals.append(columns[1])

        elif pred_type in RANGE_PREDS:
            vals = [None, None]
            col_name, val_loc, val = parse_column(pred, pred_type)
            vals[val_loc] = val

            # this loop may find no matching predicate for the other side, in
            # which case, we just leave the val as None
            for pred2 in pred_vals:
                pred2_type = list(pred2.keys())[0]
                if pred2_type in RANGE_PREDS:
                    col_name2, val_loc2, val2 = parse_column(pred2, pred2_type)
                    if col_name2 == col_name:
                        # assert val_loc2 != val_loc
                        if val_loc2 == val_loc:
                            # same predicate as pred
                            continue
                        vals[val_loc2] = val2
                        break

            predicate_types.append("lt")
            predicate_cols.append(col_name)
            if "g" in pred_type:
                # reverse vals, since left hand side now means upper bound
                vals.reverse()
            predicate_vals.append(vals)

        elif pred_type == "between":
            # we just treat it as a range query
            col = pred[pred_type][0]
            val1 = pred[pred_type][1]
            val2 = pred[pred_type][2]
            vals = [val1, val2]
            predicate_types.append("lt")
            predicate_cols.append(col)
            predicate_vals.append(vals)
        elif pred_type == "in" \
                or "like" in pred_type:
            # includes preds like, ilike, nlike etc.
            column = pred[pred_type][0]
            # what if column has been seen before? Will just be added again to
            # the list of predicates, which is the correct behaviour
            vals = pred[pred_type][1]
            if isinstance(vals, dict):
                vals = vals["literal"]
            if not isinstance(vals, list):
                vals = [vals]
            predicate_types.append(pred_type)
            predicate_cols.append(column)
            predicate_vals.append(vals)
        else:
            # TODO: need to support "OR" statements
            return None
            # assert False, "unsupported predicate type"

    start = time.time()
    predicate_cols = []
    predicate_types = []
    predicate_vals = []
    parsed_query = parse(query)
    pred_vals = get_all_wheres(parsed_query)

    # print("starting extract predicate cols!")
    for i, pred in enumerate(pred_vals):
        assert len(pred.keys()) == 1
        pred_type = list(pred.keys())[0]
        _parse_predicate(pred, pred_type)

    # print("extract predicate cols done!")
    # print("extract predicates took ", time.time() - start)
    return predicate_cols, predicate_types, predicate_vals

def extract_from_clause(query):
    '''
    Optimized version using sqlparse.
    Extracts the from statement, and the relevant joins when there are multiple
    tables.
    @ret: froms:
          froms: [alias1, alias2, ...] OR [table1, table2,...]
          aliases:{alias1: table1, alias2: table2} (OR [] if no aliases present)
          tables: [table1, table2, ...]
    '''
    def handle_table(identifier):
        table_name = identifier.get_real_name()
        alias = identifier.get_alias()
        tables.append(table_name)
        if alias is not None:
            from_clause = ALIAS_FORMAT.format(TABLE = table_name,
                                ALIAS = alias)
            froms.append(from_clause)
            aliases[alias] = table_name
        else:
            froms.append(table_name)

    start = time.time()
    froms = []
    # key: alias, val: table name
    aliases = {}
    # just table names
    tables = []

    start = time.time()
    parsed = sqlparse.parse(query)[0]
    # let us go over all the where clauses
    from_token = None
    from_seen = False
    for token in parsed.tokens:
        # print(type(token))
        # print(token)
        if from_seen:
            if isinstance(token, IdentifierList) or isinstance(token,
                    Identifier):
                from_token = token
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True

    assert from_token is not None
    if isinstance(from_token, IdentifierList):
        for identifier in from_token.get_identifiers():
            handle_table(identifier)
    elif isinstance(from_token, Identifier):
        handle_table(from_token)
    else:
        assert False

    return froms, aliases, tables

def find_next_match(tables, wheres, index):
    '''
    ignore everything till next
    '''
    match = ""
    _, token = wheres.token_next(index)
    if token is None:
        return None, None
    # FIXME: is this right?
    if token.is_keyword:
        index, token = wheres.token_next(index)

    tables_in_pred = find_all_tables_till_keyword(token)
    assert len(tables_in_pred) <= 2

    token_list = sqlparse.sql.TokenList(wheres)

    while True:
        index, token = token_list.token_next(index)
        if token is None:
            break
        # print("token.value: ", token.value)
        if token.value == "AND":
            break

        match += " " + token.value

        if (token.value == "BETWEEN"):
            # ugh ugliness
            index, a = token_list.token_next(index)
            index, AND = token_list.token_next(index)
            index, b = token_list.token_next(index)
            match += " " + a.value
            match += " " + AND.value
            match += " " + b.value
            # Note: important not to break here! Will break when we hit the
            # "AND" in the next iteration.

    # print("tables: ", tables)
    # print("match: ", match)
    # print("tables in pred: ", tables_in_pred)
    for table in tables_in_pred:
        if table not in tables:
            # print(tables)
            # print(table)
            # pdb.set_trace()
            # print("returning index, None")
            return index, None

    if len(tables_in_pred) == 0:
        return index, None

    return index, match
def find_all_clauses(tables, wheres):
    matched = []
    # print(tables)
    index = 0
    while True:
        index, match = find_next_match(tables, wheres, index)
        # print("got index, match: ", index)
        # print(match)
        if match is not None:
            matched.append(match)
        if index is None:
            break

    return matched

def find_all_tables_till_keyword(token):
    tables = []
    # print("fattk: ", token)
    index = 0
    while (True):
        if (type(token) == sqlparse.sql.Comparison):
            left = token.left
            right = token.right
            if (type(left) == sqlparse.sql.Identifier):
                tables.append(left.get_parent_name())
            if (type(right) == sqlparse.sql.Identifier):
                tables.append(right.get_parent_name())
            break
        elif (type(token) == sqlparse.sql.Identifier):
            tables.append(token.get_parent_name())
            break
        try:
            index, token = token.token_next(index)
            if ("Literal" in str(token.ttype)) or token.is_keyword:
                break
        except:
            break

    return tables

def cached_execute_query(sql, user, db_host, port, pwd, db_name,
        pre_execs, execution_cache_threshold,
        sql_cache_dir=None):
    '''
    @db_host: going to ignore it so default localhost is used.
    @pre_execs: options like set join_collapse_limit to 1 that are executed
    before the query.

    executes the given sql on the DB, and caches the results in a
    persistent store if it took longer than self.execution_cache_threshold.
    '''
    sql_cache = None
    if sql_cache_dir is not None:
        assert isinstance(sql_cache_dir, str)
        sql_cache = klepto.archives.dir_archive(sql_cache_dir,
                cached=True, serialized=True)

    hashed_sql = deterministic_hash(sql)

    # archive only considers the stuff stored in disk
    if sql_cache is not None and hashed_sql in sql_cache.archive:
        # load it and return
        # print("loaded {} from cache".format(hashed_sql))
        return sql_cache.archive[hashed_sql]

    start = time.time()

    # FIXME: this needs consistent handling
    os_user = getpass.getuser()
    if os_user == "ubuntu":
        # for aws
        con = pg.connect(user=user, port=port,
                password=pwd, database=db_name)
    else:
        # for chunky
        con = pg.connect(user=user, host=db_host, port=port,
                password=pwd, database=db_name)

    cursor = con.cursor()

    for setup_sql in pre_execs:
        cursor.execute(setup_sql)

    try:
        cursor.execute(sql)
    except Exception as e:
        cursor.execute("ROLLBACK")
        con.commit()
        cursor.close()
        con.close()
        if not "timeout" in str(e):
            print("failed to execute for reason other than timeout")
            print(e)
            pdb.set_trace()
        return None

    exp_output = cursor.fetchall()
    cursor.close()
    con.close()
    end = time.time()
    if (end - start > execution_cache_threshold) \
            and sql_cache is not None:
        sql_cache.archive[hashed_sql] = exp_output
    return exp_output

def deterministic_hash(string):
    return int(hashlib.sha1(str(string).encode("utf-8")).hexdigest(), 16)
