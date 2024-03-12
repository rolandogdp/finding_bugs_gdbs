#!/usr/bin/env python3
import string
import re
import configparser
from random import randint, choice,uniform,shuffle
import graph_tool.all as gt

# this is a lightweight cypher query generator

class RandomCypherGenerator_subqueries_with_graph():
    # in GraphGenie, the high-level idea is to mutated the graph query pattern
    # that is, the _path, rather than _predicate mutated by many existing works
    cypher_query_pattern = "{_match} {_path} {_predicate} {_return} {_other}"

    # Note: we generate cypher queries in an incremental way
    # we start from a specific number (node_number) which limits the count of nodes
    # self._node_num = int(config['testing_configs']['_node_num'])
    # You can start from two nodes so it only generates (x)-[y]-(z)
    # path vectors store the previously tested graph query patterns
    # if we test duplicated patterns for many times (recorded in `stuck`)
    # we would increase the node number, now the condition is
    # self.stuck==2*self._node_num*self._node_num
    _path_vectors = []
    _last_vector_length = 0
    stuck = 0

    # cypher query elements
    _match = ""
    _path = ""
    _predicate = ""
    _return = ""
    _other = ""

    def __init__(self, node_labels, edge_labels, node_properties, connectivity_matrix, property_types_dict,graph_full ):
        config = configparser.ConfigParser()
        config.read('graphgenie.ini')
        self.graphdb = config['default']['graphdb']
        self.language = config['default']['language']
        self._node_num = int(config['testing_configs']['_node_num'])
        self.min_node_num = int(config['query_generation_args']['min_node_num'])
        self.max_node_num = int(config['query_generation_args']['max_node_num'])
        self.variable_pathlen_rate = float(config['query_generation_args']['variable_pathlen_rate'])
        self.node_symbol_rate = float(config['query_generation_args']['node_symbol_rate'])
        self.edge_symbol_rate = float(config['query_generation_args']['edge_symbol_rate'])
        self.node_label_rate = float(config['query_generation_args']['node_label_rate'])
        self.edge_label_rate = float(config['query_generation_args']['edge_label_rate'])
        self.multi_node_label_rate = float(config['query_generation_args']['multi_node_label_rate'])
        self.multi_edge_label_rate = float(config['query_generation_args']['multi_edge_label_rate'])
        self.cyclic_rate = float(config['query_generation_args']['cyclic_rate'])
        self.random_symbol_len = int(config['query_generation_args']['random_symbol_len'])
        self.cyclic_symbol = config['query_generation_args']['cyclic_symbol']
        self.multi_node_labels = int(config['query_generation_args']['multi_node_labels'])
        self.multi_edge_labels = int(config['query_generation_args']['multi_edge_labels'])
        self.node_labels = node_labels
        self.edge_labels = edge_labels
        self.node_properties = node_properties
        self.connectivity_matrix = connectivity_matrix
        self.property_types_dict = property_types_dict
        self.graph_full = graph_full
        self.subquery_max_branching = 4
        # We create a view with only vertices with at least one edge.
        self.graph_full_view = gt.GraphView(self.graph_full, vfilt=lambda v: v.out_degree() > 0)
        print(f"INIT:{node_labels}, edge_labels{edge_labels},connectivity_matrix{connectivity_matrix},property_types_dict{property_types_dict},")
        

    # call before each run of test
    def init(self):
        config = configparser.ConfigParser()
        config.read('graphgenie.ini')
        self._node_num = int(config['testing_configs']['_node_num'])
        self._path_vectors = []
        self._last_vector_length = 0
        self.stuck = 0
        self.graph_path = []
        self.labels = []
        self.init_query()

    # note: call before each generation
    def init_query(self):
        self._match = ""
        self._path = ""
        self._predicate = ""
        self._return = ""
        self._condition = ""
        self.symbols = []
        self.symbolsids = []
        self.node_symbols = []
        self.edge_symbols = []
        self.name_label_dict = {}
        self.name_label_types_dict = {}
        self.nodes_num = 0

        self.property_to_test = ""
        

    # this is a random choice api for a given rate
    # e.g., if given_rate = 0.3, then 30% returns true and 70% return false
    def random_choice(self, given_rate):
        if randint(1,100)<=given_rate*100:
            return True
        else:
            return False

    # this is a random symbol generator
    # note: we only consider lowercase letters in ascii
    def random_symbol(self):
        return ''.join(choice(string.ascii_lowercase) for _ in range(self.random_symbol_len))

    # _match indicates the query is a graph-matching query rather than add/update/delete queries
    # cypher: `match` or `optional match` clause
    def match_generator(self):
        match_candidates = ["MATCH"]#, "OPTIONAL MATCH"]
        self._match = choice(match_candidates)

    def random_node_multi_labels(self):
        label_num = len(self.node_labels)
        random_num = randint(2, label_num)
        node_label = choice(self.node_labels)
        for i in range(random_num-1):
            node_label += "|{}".format(choice(self.node_labels))
        return node_label

    def random_edge_types(self):
        type_num = len(self.edge_labels)
        random_num = randint(2, type_num)
        edge_type = choice(self.edge_labels)
        for i in range(random_num-1):
            edge_type += "|{}".format(choice(self.edge_labels))
        return edge_type

    # given the previous node, use connectivity matrix to find connectable node labels
    def connectable_node_labels(self, prev_node_label, prev_node_direction):
        if self.graphdb!="neo4j":
            return self.node_labels
        if prev_node_label==None or prev_node_label=="" or "%" in self.node_labels:
            return self.node_labels
        possible_node_labels = []
        for each_prev_node_label in prev_node_label.split('|'):
            prev_node_index = self.node_labels.index(each_prev_node_label)
            for each_label in self.node_labels:
                each_label_index = self.node_labels.index(each_label)
                if prev_node_direction==">":
                    if self.connectivity_matrix[prev_node_index][each_label_index]!=0:
                        possible_node_labels.append(each_label)
                elif prev_node_direction=="<":
                    if self.connectivity_matrix[each_label_index][prev_node_index]!=0:
                        possible_node_labels.append(each_label)
                else:
                    if self.connectivity_matrix[prev_node_index][each_label_index]!=0 or self.connectivity_matrix[each_label_index][prev_node_index]!=0:
                        possible_node_labels.append(each_label)
        return possible_node_labels

    # to generate random path unit
    def random_path_unit(self, prev_node_label=None, prev_node_direction=None):
        connectable_node_labels = self.connectable_node_labels(prev_node_label, prev_node_direction)
        path_unit_candidates = [
            "({node_sym})-[{edge_sym}]-",
            "({node_sym})<-[{edge_sym}]-",
            "({node_sym})-[{edge_sym}]->"
            ]
        random_unit = choice(path_unit_candidates)
        random_node_sym = self.random_symbol() if self.random_choice(self.node_symbol_rate) else ""
        random_edge_sym = self.random_symbol() if self.random_choice(self.edge_symbol_rate) else ""
        # determine whether we need node label
        if self.random_choice(self.node_label_rate) and random_node_sym!="" and len(connectable_node_labels)!=0:
            node_labels = ""
            if self.graphdb=="neo4j" and self.multi_node_labels:
                node_labels = self.random_node_multi_labels() if self.random_choice(self.multi_node_label_rate) else choice(connectable_node_labels)
            else:
                node_labels = choice(connectable_node_labels)
            random_node_sym = "{}:{}".format(random_node_sym, node_labels)
        # determine whether we need edge label
        if self.random_choice(self.edge_label_rate) and random_edge_sym!="":
            # do not support multiple edge labels
            random_edge_sym = "{}:{}".format(random_edge_sym, choice(self.edge_labels))
        return random_unit.format(node_sym=random_node_sym, edge_sym=random_edge_sym)

    def cypher_get_unit_direction(self, path_unit):
        if "<" in path_unit:
            return "<"
        elif ">" in path_unit:
            return ">"
        else:
            return "-"

    def parse_path_unit_node_label(self, path_unit):
        the_node = path_unit.split('-')[0]
        if ':' not in the_node:
            return ""
        else:
            # extract Person from (a:Person)
            return the_node.split(':')[1].split(')')[0]

    # it is important to generate diverse graph query patterns
    def path_generator(self):
        nodes_num = self._node_num
        path = ""
        prev_node_label = ""
        prev_node_direction = "-"
        # given the number of nodes, we generate each node unit
        # it takes previous node label and edge information and checks the connectivity
        # connectivity: before testing, we have parsed the target dataset to pre-analyze
        # the connectivity among different types of nodes
        # note: if supporting update/insert clauses later, we need incremental updates to
        # the connectivity matrix
        for i in range(nodes_num):
            new_path_unit = self.random_path_unit(prev_node_label, prev_node_direction)
            # update the previous node label and edge direction after generation
            prev_node_label = self.parse_path_unit_node_label(new_path_unit)
            prev_node_direction = self.cypher_get_unit_direction(new_path_unit)
            if self.random_choice(self.variable_pathlen_rate):
                variable_length_expressions = ["*..1]", "*0..1]", "*0..0]", "*1..1]"]
                new_path_unit = new_path_unit.replace(']', choice(variable_length_expressions))
            path += new_path_unit
        # to strip the tail edge
        path = ")".join(path.split(")")[:-1])+")"
        # to generate cyclic path
        if self.random_choice(self.cyclic_rate):
            cyclic_str = "{cyc_sym}{node_label}".format(
                cyc_sym = self.cyclic_symbol,
                node_label= ":"+choice(self.node_labels) if self.random_choice(self.node_label_rate) else ""
            )
            path = ("({cyc})-{path}-({cyc})".format(cyc=cyclic_str, path="-".join(path.split("-")[1:-1])))
        self._path = path
        self.path_parser()
    


    def path_parser(self):
        self.nodes_num = self._path.count('-')/2 + 1
        symbol_list = re.findall(r"[(,\[][a-z]{{{sym_len}}}".format(sym_len=self.random_symbol_len), self._path)
        symbol_list_with_labels = re.findall(r"[(,\[][a-z]{{{sym_len}}}[:]?[\w]+".format(sym_len=self.random_symbol_len), self._path)
        for each_symbol in symbol_list:
            symbol_name = each_symbol[1:]
            self.symbols.append(symbol_name)
            if each_symbol[0]=="(":
                self.node_symbols.append(symbol_name)
            elif each_symbol[0]=="[":
                self.edge_symbols.append(symbol_name)
        for each_symbol in symbol_list_with_labels:
            symbol_name = each_symbol[1:].split(':')[0]
            symbol_label = each_symbol[1:].split(':')[1]
            self.symbols.append(symbol_name)
            if each_symbol[0]=="(":
                self.node_symbols.append(symbol_name)
            elif each_symbol[0]=="[":
                self.edge_symbols.append(symbol_name)
            self.name_label_dict.update({symbol_name: symbol_label})
        # to record the path, for incrementally generation
        # record node labels only, save as vector.
        all_nodes = re.findall(r"\([A-Za-z0-9:]*\)", self._path)
        path_vector = []
        for each_node in all_nodes:
            if ':' not in each_node:
                path_vector.append(0)
            else:
                node_label = each_node.split(':')[1].split(')')[0]
                path_vector.append(self.node_labels.index(node_label)+1)

        # the code below is for incremental base query generation
        # we encode the graph pattern into vectors
        # too many deduplicated queries would increase the node number of graph pattern

        if path_vector not in self._path_vectors:
            print("node num: {} tested vectors:{}".format(self._node_num, self._last_vector_length+1))
            self._path_vectors.append(path_vector)
        if self._last_vector_length == len(self._path_vectors):
            self.stuck += 1
        else:
            self.stuck = 0
        self._last_vector_length = len(self._path_vectors)
        if self.stuck==2*self._node_num*self._node_num:
            self.stuck = 0
            self._node_num += 1
            self._last_vector_length = 0
            self._path_vectors.clear()

    def get_operator(self, property_type,left_side='') -> tuple[str, list[str, None]]:
        print(f" property_type: {property_type},left side:{left_side}")
        # ONLY FOR NEO4J AT THE MOMENT
        # TODO: add more operators?
        # TODO: add more property types? https://neo4j.com/docs/python-manual/current/data-types/#_core_types
        #   https://neo4j.com/docs/cypher-manual/current/functions/scalar/#functions-valueType
        #
        # All the types are the following:
        # NOTHING
        # NULL
        # BOOLEAN
        # STRING
        # INTEGER
        # FLOAT
        # DATE
        # LOCAL TIME
        # ZONED TIME
        # LOCAL DATETIME
        # ZONED DATETIME
        # DURATION
        # POINT
        # NODE
        # RELATIONSHIP
        ret = f"{left_side} IS NOT NULL" # Default case
        right_side_type = None
        print(f"DID I FAIL? {ret} ")
        if "NOTHING" in property_type:
            # Not much to do here..?
            return( "IS NOTHING", None)
        elif "BOOLEAN" in property_type:
            # Min: False, Max: True
            if self.random_choice(0.5):
                return( f"{left_side} = True", None)
            else:
                return( f"{left_side} = False", None)
        elif "STRING" in property_type:# Be more creative here I guess?
            # Min: "", Max:
            OPERATORS_STRING = ["STARTS WITH", "ENDS WITH", "CONTAINS"]
            pass
        elif "INTEGER" in property_type:
            # Min: Long.MIN_VALUE, Max: Long.MAX_VALUE in java so
            # Min: -2^63, Max: 2^63-1
            OPERATORS_INTEGER = ["=", ">", "<", ">=", "<="]
            operator = choice(OPERATORS_INTEGER)
            right_side = randint(-2**63, 2**63-1)
            return( f"{left_side} {operator} {right_side}", None)
            
        elif "FLOAT" in property_type:
            
            # Double.MIN_VALUE Double.MAX_VALUE
            # Min: 4.9e-324, Max: 1.7976931348623157e+308
            OPERATORS_FLOAT = ["=", ">", "<", ">=", "<="]
            operator = choice(OPERATORS_FLOAT)
            right_side = uniform(4.9e-324, 1.7976931348623157e+308)
            return( f"{left_side} {operator} {right_side}", None)

        elif "DATE" in property_type:
            # Min: -999_999_999-01-01, Max: +999_999_999-12-31
            # Use neo4j.time import Date

            pass
        elif "LOCAL TIME" in property_type:
            pass
        elif "ZONED TIME" in property_type:
            pass
        elif "LOCAL DATETIME" in property_type:
            pass
        elif "ZONED DATETIME" in property_type:
            pass
        elif "DURATION" in property_type:
            # Min:P-292471208677Y-6M-15DT-15H-36M-32, Max: P292471208677Y6M15DT15H36M32.999999999S
            pass
        elif "POINT" in property_type:
            pass
        elif "NODE" in property_type:
            pass
        elif "RELATIONSHIP" in property_type:
            pass

        elif "NULL" in property_type:
            # Is null or not null?
            if self.random_choice(0.5):
                return( f"{left_side} IS NOT NULL", None)
            else:
                return( f"{left_side} IS NULL", None)
        else:
            # G
            pass
        return( ret, right_side_type)
    


    def predicate_generator_property_test(self,id_to_test=None):
        if id_to_test == None:
            id_to_test = choice(self.symbolsids) if len(self.symbolsids)>0 else ""
        # print("====TEST"*100)
        # print(self.symbolsids)
        # print(len(self.symbolsids))
        conditions = []
        
            
        # id_to_test= symbol
        if id_to_test != "":
            id_int  = int(id_to_test[2:]) #id1 -> 1

            predicate_placeholder = "( {id}.{property} {operator} {value} )"

            property_to_test = "id"
            value = id_int
            # Fancier way to do it but may cause problem with urls or other stuff... Let's keep it simple for now with only ids

            # node = self.graph_full.vertex(id_int)
            # property_to_test = choice(list(self.graph_full.vertex_properties["properties"][node].keys()))
            
            # value = self.graph_full.vertex_properties["properties"][node][property_to_test]
            predicate = predicate_placeholder.format(id=id_to_test,property=property_to_test,operator="=",value=value)
            
        else: predicate = "True"
        return predicate
        
    # TODO: add more predicate
    def predicate_generator(self):
        
        pattern = "WHERE {}"
        
        ## Properties tests
        # should_test_propertyQ = self.random_choice(0.5) # For later, when 
    
        ### New code based on the graph
        # number_of_test = randint(0,4)
        id_to_test = choice(self.symbolsids) if len(self.symbolsids)>0 else ""

        conditions = []
        for symbol in  self.symbolsids:
            predicate_test = self.predicate_generator_property_test(id_to_test=symbol)
            conditions.append(predicate_test)
                
        predicate = " AND ".join(conditions) if len(conditions)>0 else "True"
        condition = predicate

        ## Subqueries   
             
        subqueries = []
        self.number_nested_predicates = randint(0,4) # TODO: Add a parameter for this

        # EXISTs subqueries
        if self.number_nested_predicates > 0:
            pattern = "WHERE {conditions} AND EXISTS {subquery}"

            # choose randomly between nested subqueries, UNION subqueries, WITH subqueries:
            choosed_subquery_type = choice(["nested","union"])#,"with"])
            # choosed_subquery_type = "with"
            if choosed_subquery_type == "nested":
            
                nested_generator = RandomCypherGenerator_subqueries_nested(node_labels=self.node_labels,edge_labels=self.edge_labels,node_properties=self.node_properties,
                                                                        connectivity_matrix=self.connectivity_matrix, property_types_dict=self.property_types_dict,
                                                                            recursion_level=self.number_nested_predicates,graph_full=self.graph_full,graph_full_view=self.graph_full_view
                                                                            )
                for _ in range(randint(1,self.subquery_max_branching)): # Number of nested subqueries
                    
                    predicate_subqueries =  nested_generator.predicate_generator_recursiv( iterations_left=self.number_nested_predicates) 
                    if predicate_subqueries[0] != "{":
                        predicate_subqueries = "{{ {} }}".format(predicate_subqueries)
                    subqueries.append(predicate_subqueries)
            elif choosed_subquery_type == "union":
                # Generate subqueries with union
                for _ in range(randint(1,self.subquery_max_branching)):
                    subquery_for_union = self.union_generator()
                    subqueries.append(subquery_for_union)
            # elif choosed_subquery_type == "with":
            #     #Generate subqueries with with
            #     for _ in range(randint(1,self.subquery_max_branching)):
            #         subquery_for_with = self.with_generator()
            #         if subquery_for_with[0] != "{":
            #             subquery_for_with = "{{ {} }}".format(subquery_for_with)

            #         subqueries.append(subquery_for_with)

            
            subquery = " AND EXISTS ".join(subqueries)
            self._predicate = pattern.format(conditions=condition,subquery=subquery)

            return

            
        else:
            predicate = "{} IS NOT NULL AND True".format(choice(self.node_symbols)) if len(self.node_symbols)>0 else "True"
        self._predicate = pattern.format(predicate)

    # note: we focus on testing `count`
    def return_generator(self):
        _return = "{return_keyword} {return_staff}"
        return_keywords = ["RETURN", "RETURN DISTINCT"]
        # TODO: to support count(DISTINCT ), max(), min()
        test_returns = ["count({})"]
        if len(self.symbolsids)>0:# and self.random_choice(0.5):
            return_staff = choice(self.symbolsids)
        else:
            return_staff = choice(test_returns).format(choice(self.symbols)) if len(self.symbols)>0 else "count(1)"
        self._return = _return.format(
            return_keyword = choice(return_keywords),
            return_staff = return_staff
        )

    # for count() testing, we do not really need other clauses
    def other_generator(self):
        _other = "{order_by} {skip} {limit}"
        order_by_keywords = ["", "ORDER BY -1+1", "ORDER BY NULL"]
        skip_keywords = ["", "SKIP 0", "SKIP 0", "SKIP 0", "SKIP 0", "SKIP 0", "SKIP 0", "SKIP 0",]
        limit_keywords = ["", "LIMIT 1", "LIMIT 2", "LIMIT 3", "LIMIT 4","LIMIT 5"]
        self._other = _other.format(
            order_by = choice(order_by_keywords),
            skip = choice(skip_keywords),
            limit = choice(limit_keywords)
        )


    def get_random_path_in_graph(self,starting_vertice=None):
        # TODO: optimize selection of starting vertice using additional graph metrics to guarantee finding a long enough path.
        found = False
        i=0
        choosen_path = []
        while not found:
            if starting_vertice == None:
                starting_vertice = choice(self.graph_full_view.get_vertices())
            visitorResult = VisitorResult()
            visitor = VisitorExample(self.graph_full.vertex_properties["properties"],visitorResult,path_min=1,path_max=self.max_node_num)
            gt.dfs_search(self.graph_full, starting_vertice,visitor )
            for path in visitorResult.final_path[::-1]:
                if len(path) >= self.min_node_num:
                    found = True
                    choosen_path = path
                    break

            if i > 1000:
                print("COULD NOT FIND A PATH")
                # shuffle(visitorResult.final_path.sort(key=len,reverse=True)) # We can add additional randomness here
                visitorResult.final_path.sort(key=len,reverse=True)
                choosen_path = visitorResult.final_path[0]
                found = True                
            i+=1
        
        return choosen_path
        
    def path_generator_graph(self):
        graph_path = self.get_random_path_in_graph()
        # print("PATH:",path)
        
        path = ""
        path_units= "({node_sym})-[{edge_sym}]->"
        for i in range(len(graph_path)-1):
            
            node = graph_path[i]
            next_node = graph_path[i+1]
            
            # print("ICIIIIII:",self.graph_full.vertex_properties["properties"])
            random_node_sym1 = "id"+str(self.graph_full.vertex_properties["properties"][node]["id"])# if self.random_choice(self.node_symbol_rate) else ""
            random_node_label1 = ":"+choice(self.graph_full.vertex_properties["labels"][node]) #if self.random_choice(self.multi_node_label_rate) else ""
            random_node_sym = "{}{}".format(random_node_sym1,random_node_label1 )
            edge_label = self.graph_full.edge_properties["properties"][self.graph_full.edge(node,next_node)]    
            #if self.random_choice(self.multi_edge_label_rate):
            #     random_edge_label = edge_label
            # else: 
            #     random_edge_label = ""
            random_edge_label = edge_label
        
            if random_node_sym == ":":
                random_node_sym = ""
            
            random_edge_sym = ":{}".format(random_edge_label )
            self.symbolsids.append(random_node_sym1)

            path += path_units.format(node_sym=random_node_sym,edge_sym=random_edge_sym)
        
        random_node_sym1 = "id"+str(self.graph_full.vertex_properties["properties"][next_node]["id"]) #self.random_symbol() if self.random_choice(self.node_symbol_rate) else ""
        random_node_label1 = ":"+choice(self.graph_full.vertex_properties["labels"][next_node]) #if self.random_choice(self.multi_node_label_rate) else ""
        random_node_sym = "{}{}".format(random_node_sym1,random_node_label1 )
        self.symbolsids.append(random_node_sym1)
        
        path_final = "({node_sym})".format(node_sym=random_node_sym)
        path = path + path_final
        self._path = path
        # print("\n\n\n\n HERE PATH:",path)
        # self.path_parser() # TODO: Unsure..?

    def union_generator(self):
        #Generate subqueries with union
        subqueries = []
        nested_generator = RandomCypherGenerator_subqueries_nested(node_labels=self.node_labels,edge_labels=self.edge_labels,node_properties=self.node_properties,
                                                                       connectivity_matrix=self.connectivity_matrix, property_types_dict=self.property_types_dict,
                                                                         recursion_level=self.number_nested_predicates,graph_full=self.graph_full,graph_full_view=self.graph_full_view)
        self.number_nested_predicates = randint(0,4)
        number_of_unions = randint(0,4)   
        for _ in range(number_of_unions): # Number of union
            
            subquery_for_union = nested_generator.predicate_generator_recursiv(iterations_left=self.number_nested_predicates)
            subqueries.append(subquery_for_union)
            print("SUBQUERY UNION: ", subquery_for_union)
            subquery_for_union = subquery_for_union.lstrip("{").rstrip("}")
            # input("Waiting")
        

        union_subquery = " UNION ".join(subqueries)
        if union_subquery!="" and union_subquery[0] != "{":
            union_subquery = "{{ {} }}".format(union_subquery)

        print("UNION SUBQUERY: ",union_subquery)

        return union_subquery
    
    def with_generator(self):
        #Generate subqueries with with random symbols not used in this level nor nested levels.
        subqueries = []
        nested_generator = RandomCypherGenerator_subqueries_nested(node_labels=self.node_labels,edge_labels=self.edge_labels,node_properties=self.node_properties,
                                                                       connectivity_matrix=self.connectivity_matrix, property_types_dict=self.property_types_dict,
                                                                         recursion_level=self.number_nested_predicates,graph_full=self.graph_full,graph_full_view=self.graph_full_view)
        self.number_nested_predicates = randint(0,4)
        number_of_withs = randint(0,4)
        with_subquery = "WITH {name} AS \"{variable}\" {subquery}"   
         # Number of with
            
        subquery_for_with = nested_generator.predicate_generator_recursiv(iterations_left=self.number_nested_predicates)

        name = self.random_symbol()
        variable = self.random_symbol()

        subquery_for_with = subquery_for_with.lstrip("{").rstrip("}")
        subquery = with_subquery.format(
            name = name,
            variable = variable,
            subquery = subquery_for_with 
        )
        

        return subquery

    def random_query_generator(self):
        self.init_query()
        self.match_generator()
        self.path_generator_graph()
        self.predicate_generator()
        self.return_generator()
        self.other_generator()
        
        query = self.cypher_query_pattern.format(
            _match = self._match,
            _path = self._path,
            _predicate = self._predicate,
            _return = self._return,
            _other = self._other
        )
        # query = re.sub(' +', ' ', query).strip(' ')
        # query = re.sub('[*]+', '*', query).strip(' ')
        return query





        
        


class VisitorResult():
    def __init__(self) -> None:
        self.final_path = []
    
class VisitorExample(gt.DFSVisitor):

    def __init__(self,properties,visitorResult:VisitorResult,path_min=1,path_max=5):
        self.properties = properties
        self.finished_paths = []
        self.path_max = path_max
        self.path_min = path_min
        self.visitorResult = visitorResult
        
    def start_vertex(self, u):
        
        number_of_childs = u.out_degree()
        # print("Starting with:",u, " with ",number_of_childs," childs")
        self.current_path = [[u] for _ in range(number_of_childs)]
        # print("Initial Current path:",self.current_path)
        
        
    # def discover_vertex(self, u):
    #     self.current_path.append(u)
    
    def examine_edge(self, e):
        # print("examine_edge",e.source(),e.target())
        # print("current path:",self.current_path)
        for path in self.current_path[::-1]:
            # print(e.source(),path)
            if e.source() == path[-1]:
                # print("YAY")
                path.append(e.target())
                number_of_childs = e.target().out_degree()
                for _ in range(number_of_childs-1):
                    self.current_path.append(path.copy())
                    if len(path) >= self.path_max:
                        self.visitorResult.final_path = path
                        raise gt.StopSearch()
                
                break
        else:
            print("NOOO")
            print(e.source()), print(path[-1])

    def finish_vertex(self, u):
        # print("finish vertex")

        for path in self.current_path[::-1]:
            # print(type(u))
            # print(type(path[-1]))
            if u == path[-1]:
                self.finished_paths.append(path)
                self.visitorResult.final_path = self.finished_paths
                
                break
                
        # print("FINAL PATH:",[self.properties[u]["id"] for u in self.current_path[-1]])
        # print(len(self.current_path))
        # print("Finished paths:",self.finished_paths)
        # print("==")


class RandomCypherGenerator_subqueries_nested(RandomCypherGenerator_subqueries_with_graph):
    def __init__(self, node_labels, edge_labels, node_properties, connectivity_matrix, property_types_dict,recursion_level,graph_full,graph_full_view ):
        config = configparser.ConfigParser()
        config.read('graphgenie.ini')
        self.graphdb = config['default']['graphdb']
        self.language = config['default']['language']
        self._node_num = int(config['testing_configs']['_node_num'])
        self.min_node_num = int(config['query_generation_args']['min_node_num'])
        self.max_node_num = int(config['query_generation_args']['max_node_num'])
        self.variable_pathlen_rate = float(config['query_generation_args']['variable_pathlen_rate'])
        self.node_symbol_rate = float(config['query_generation_args']['node_symbol_rate'])
        self.edge_symbol_rate = float(config['query_generation_args']['edge_symbol_rate'])
        self.node_label_rate = float(config['query_generation_args']['node_label_rate'])
        self.edge_label_rate = float(config['query_generation_args']['edge_label_rate'])
        self.multi_node_label_rate = float(config['query_generation_args']['multi_node_label_rate'])
        self.multi_edge_label_rate = float(config['query_generation_args']['multi_edge_label_rate'])
        self.cyclic_rate = float(config['query_generation_args']['cyclic_rate'])
        self.random_symbol_len = int(config['query_generation_args']['random_symbol_len'])
        self.cyclic_symbol = config['query_generation_args']['cyclic_symbol']
        self.multi_node_labels = int(config['query_generation_args']['multi_node_labels'])
        self.multi_edge_labels = int(config['query_generation_args']['multi_edge_labels'])
        self.node_labels = node_labels
        self.edge_labels = edge_labels
        self.node_properties = node_properties
        self.connectivity_matrix = connectivity_matrix
        self.property_types_dict = property_types_dict
        # print(f"INIT:{node_labels}, edge_labels{edge_labels},connectivity_matrix{connectivity_matrix},property_types_dict{property_types_dict},")
        self.recursion_level = recursion_level

        self.graph_full = graph_full
        # We create a view with only vertices with at least one edge.
        self.graph_full_view = graph_full_view
        self.subquery_max_branching = 1 #TODO: Add a parameter for this
    
    def generate_condition(self):
        
        ## Properties tests
        # should_test_propertyQ = self.random_choice(0.5) # For later, when 
    
        ### New code based on the graph
        # number_of_test = randint(0,4)
        id_to_test = choice(self.symbolsids) if len(self.symbolsids)>0 else ""

        conditions = []
        for symbol in  self.symbolsids:
            predicate_test = self.predicate_generator_property_test(id_to_test=symbol)
            conditions.append(predicate_test)
                
        predicate = " AND ".join(conditions) if len(conditions)>0 else "True"
    
        return predicate

       # TODO: add more predicate

    def predicate_generator(self):
        
        pattern = "WHERE {}"
        
        # should_test_propertyQ = self.random_choice(0.5) # For later, when 
        should_test_propertyQ = True
        if should_test_propertyQ:
            ### New code based on the graph
            # number_of_test = randint(0,4)
            id_to_test = choice(self.symbolsids) if len(self.symbolsids)>0 else ""
            # print("====ICI"*100)
            # print(self.symbolsids)
            # print(len(self.symbolsids))
            conditions = []
            for symbol in  self.symbolsids:
                
                id_to_test= symbol
                if id_to_test != "":
                    id_int  = int(id_to_test[2:]) #id1 -> 1

                    predicate_placeholder = "( {id}.{property} {operator} {value} )"

                    property_to_test = "id"
                    value = id_int
                    # Fancier way to do it but may cause problem with urls or other stuff... Let's keep it simple for now with only ids

                    # node = self.graph_full.vertex(id_int)
                    # property_to_test = choice(list(self.graph_full.vertex_properties["properties"][node].keys()))
                    
                    # value = self.graph_full.vertex_properties["properties"][node][property_to_test]
                    predicate = predicate_placeholder.format(id=id_to_test,property=property_to_test,operator="=",value=value)
                    # print("COUCOU: ",predicate)
                   

                else:
                    predicate = "True"
                conditions.append(predicate)
                # print("FOR LOOP CONDITIONS: ",conditions)
        predicate = " AND ".join(conditions) if len(conditions)>0 else "True"
        condition = predicate
        
        self._predicate = pattern.format(condition)
        return self._predicate
        
      
    def predicate_generator_recursiv(self, iterations_left:int):
        # print("ITERATIONS LEFT: ",iterations_left)
        self.init_query()
        self.match_generator()
        self.path_generator_graph()
        
        #self.return_generator() #  Disabled return in subqueries for now
        self.other_generator()
        match = self._match
        path = self._path
        predicate = self._predicate
        # ret = self._return #  Disabled return in subqueries for now
        ret = ""
        other = self._other
        # print("INSIDE:",match,path,predicate,ret,other,iterations_left)
        if iterations_left <= 0: 
            sub_query = "MATCH {_path} {_predicate} {_return}"
            self.predicate_generator()
            predicate = self._predicate

            query = sub_query.format(
                _match = match,
                _path = path,
                _predicate = predicate,
                _return = ret
                # _other = other
            )

            # print("QUERY INSIDE NEsted =",query)
            
        elif iterations_left > 0:
            condition = self.generate_condition()
            if condition != "":
                condition_text = "{} AND".format(condition)
            else: condition_text = ""
            sub_query = " {_match} {_path} WHERE {_condition} EXISTS  {_predicate}  {_return} "
            predicates = []
            for _ in range(0,randint(1,self.subquery_max_branching)):
                predicate =  self.predicate_generator_recursiv(iterations_left-1)
                if predicate[0] != "{": #To avoid double brackets, we add them only if they are not already there, th
                    predicate = "{{ {} }}".format(predicate)
                predicates.append(predicate)
                # print("Iteration LEFT::",str(iterations_left)," PREDICATE added to the list: ",predicate)
            predicate = " AND EXISTS ".join(predicates)
            # print("Iteration LEFT:",str(iterations_left), "final PREDICATE: ",predicate)
            
        
            query = sub_query.format(
                _match = match,
                _path = path,
                _predicate = predicate,
                _condition = condition_text,
                _return = ret
                # _other = other
            )
        # print("QUERY INSIDE NEsted =",query)
        
        # query = re.sub(' +', ' ', query).strip(' ')
        # query = re.sub('[*]+', '*', query).strip(' ')
        # print("Iteration LEFT:",str(iterations_left),"Returning query:",query)
        return query