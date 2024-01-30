#!/usr/bin/env python3
import re
import string
import configparser
from random import choice, randint

class CypherQueryMutatorSequential:
    cypher_query_pattern = "{_match} {_path} {_predicate} {_return} {_other}"

    def __init__(self, node_labels, edge_labels, node_properties, connectivity_matrix, graph_full):
        config = configparser.ConfigParser()
        config.read('graphgenie.ini')
        self.graphdb = config['default']['graphdb']
        self.language = config['default']['language']
        self.random_symbol_len = int(config['query_generation_args']['random_symbol_len'])
        self.graph_pattern_mutation = int(config['testing_strategy']['graph_pattern_mutation'])
        self.mutated_query_num = int(config['testing_configs']['mutated_query_num'])
        self.cyclic_symbol = config['query_generation_args']['cyclic_symbol']
        self.node_labels = node_labels
        self.edge_labels = edge_labels
        self.node_properties = node_properties
        self.connectivity_matrix = connectivity_matrix
        self.graph_full = graph_full
        

    def cypher_query_parser(self, query):
        _match = "OPTIONAL MATCH" if "OPTIONAL" in query else "MATCH"
        _path = query.split("MATCH ")[1].split(' ')[0].strip(' ')
        if "WHERE " not in query:
            _predicate = ""
        else:
            _predicate = "WHERE " + query.split("WHERE")[1].split("RETURN")[0].strip(' ')
        return_symbol = "RETURN DISTINCT " if "RETURN DISTINCT" in query else "RETURN "
        _return = return_symbol + query.split(return_symbol)[1].split(')')[0]+')'
        _other = ')'.join(query.split(return_symbol)[1].split(')')[1:])
        return _match, _path, _predicate, _return, _other

    def path_parser(self, path):
        symbols = []
        node_symbols = []
        edge_symbols = []
        symbol_list = re.findall("[(,[][a-z]{{{sym_len}}}".format(sym_len=self.random_symbol_len), path)
        for each_symbol in symbol_list:
            symbol_name = each_symbol[1:]
            symbols.append(symbol_name)
            if each_symbol[0]=="(":
                node_symbols.append(symbol_name)
            elif each_symbol[0]=="[":
                edge_symbols.append(symbol_name)
        return symbols, node_symbols, edge_symbols

    def init_for_each_base_query(self, base_query):
        # meta data for base query
        self.base_query = base_query
        self.base_match = ""
        self.base_path = ""
        self.base_predicate = ""
        self.base_return = ""
        self.base_other = ""
        self.base_symbols = []
        self.base_node_symbols = []
        self.base_edge_symbols = []

        self.mutated_match = ""
        self.mutated_path = ""
        self.mutated_predicate = ""
        self.mutated_return = ""
        self.mutated_other = ""
        self.mutated_symbols = []
        self.mutated_node_symbols = []
        self.mutated_edge_symbols = []

        # we split rules into three classes: Non-GQT, Property-GQT, and Structure-GQT
        self.Non_GQT = [1,0,0]
        self.Property_GQT = [0,1,0]
        self.Structure_GQT = [0,0,1]
        self.equivalent_queries = []
        self.restricted_queries = []

        # eval list (a list of triple) records the rule using the tuple: [x,y,z]
        # x=1 indicates it includes Non-GQT transformation
        # y=1 indicates it includes Property-GQT transformation
        # z=1 indicates it includes Structure-GQT transformation
        self.equivalent_queries_eval = []
        self.restricted_queries_eval = []

        self.return_clause = ""

    def strip_spaces(self, query):
        return re.sub(" +", " ", query)

    # This is a random choice wrapper for a given rate
    # E.g., if given_rate = 0.3, then 30% returns True and 70% return False
    def random_choice(self, given_rate):
        if randint(1,100)<=given_rate*100:
            return True
        else:
            return False


    def query_parser(self, base_query):
        self.init_for_each_base_query(base_query)
        self.all_queries = self.get_all_subqueries(base_query)[::-1] # Reverse since we want to mutate from the innermost subquery
        
        self.base_match, self.base_path, self.base_predicate, self.base_return, self.base_other = self.cypher_query_parser(base_query)
        self.base_symbols, self.base_node_symbols, self.base_edge_symbols = self.path_parser(self.base_path)
        self.mutated_match, self.mutated_path, self.mutated_predicate, self.mutated_return, self.mutated_other = self.cypher_query_parser(base_query)
        self.mutated_symbols, self.mutated_node_symbols, self.mutated_edge_symbols = self.path_parser(self.mutated_path)


    def get_all_subqueries(self,query):
        
        subqueries = []
        all_queries = []
        while query !="":
            this_level_query,subquery = self.parse_subquerie(query)
            # print(f"this_level_query:{this_level_query},subquery: {subquery}")
            # print()
            all_queries.append(this_level_query.replace("   "," ").replace("  "," "))

            if subquery == "":
                break
            if "AND EXISTS":
                subqueries.append(subquery.split("AND EXISTS")[0])
            elif "WHERE EXISTS" in subquery:
                subqueries.append(subquery.split("WHERE EXISTS")[0])
            elif "EXISTS" in subquery:
                subqueries.append(subquery.split("EXISTS")[0])

            else: 
                subqueries.append(subquery)
            query = subquery
        # print("all_queries: ",all_queries)
        return all_queries

    def parse_subquerie(self,query):
        tmp = query.split("{")
        first_part = tmp[0]
        last_part = tmp[-1].split("}")[-1]

        tmp = "{".join(tmp[1:]).split("}")
        
        
        subquery = "}".join(tmp[:-1])
        # print(first_part,last_part,subquery)
        if first_part != last_part:
            this_level_query = first_part+last_part
        else:
            this_level_query = first_part
        if "AND EXISTS" in this_level_query:
            this_level_query= " ".join(this_level_query.split("AND EXISTS"))
        elif "WHERE EXISTS" in this_level_query:
            this_level_query= " ".join(this_level_query.split("WHERE EXISTS"))
        elif "EXISTS" in subquery:
            this_level_query= " ".join(this_level_query.split("EXISTS"))


        return this_level_query,subquery
    
    def generate_simple_match_view(self,subquery,view_name="view"):
        query = subquery
        if view_name !="":
            tmp = query.split("(")
            tmp[1] = f":{view_name})-[r:contains]->({tmp[1]}"
            tmp= "(".join(tmp)
            query=tmp
            print(tmp)

        if not "MATCH" in subquery: # If no match assume optional match
            query = f"OPTIONAL MATCH {subquery}"
        if not "RETURN" in subquery:
            query = f"{query} RETURN *"

        return query
    
    def generate_simple_create_view(self,subquery,new_view_name="view"):
        # print("subquery:",subquery)
        
        if "RETURN" in subquery:
            tmp = subquery.split("RETURN")
            subquery = tmp[0]
        query = subquery.lstrip()
            
            

        
        if "MATCH " in subquery:
            tmp = query.split("MATCH ")
            query = tmp[1] 
        
        
        query = re.sub(r':\w+\)', ')', query) # Remove labels from nodes for the creation. Otherwise gives error.
        query=query.replace("()-[]->(){0,1000}","")
        # print("query:",query)
        if "[r:contains]->" in query:
            query = query.split("[r:contains]->")[1]
        query = query.lstrip()
        create_query = f"CREATE ({new_view_name}:{new_view_name})-[r:contains]->{query}"
        final = f"{subquery}\n{create_query}"
        if not "MATCH" in final:
            final = f"MATCH {final}"


        return final

    def update_match(self,subquery,view_name="view"):
        if "MATCH " in subquery:
            tmp = subquery.split("MATCH ")
            tmp[1] = f"({view_name})-[r:contains]->()-[]->(){{0,1000}}{tmp[1]}"
            tmp= "MATCH ".join(tmp)
        else :
            tmp = f"MATCH ({view_name})-[r:contains]->()-[]->(){{0,1000}}{subquery}"
        
        return tmp
    def get_return_clause(self,query):
        return_clause = query.split("RETURN ")[1].split(" ")[0]
        return return_clause

    def get_all_transactions(self,base_query):
        subs = self.get_all_subqueries(base_query)
        transactions = []
        view_text ="view"
        view_id = 0
        for sub in subs:
            if view_id>0:
                sub = self.update_match(sub,curr_view)
            curr_view = view_text+str(view_id)
            print(self.generate_simple_create_view(sub,new_view_name=curr_view),"\n")
            view_id+=1
        return 