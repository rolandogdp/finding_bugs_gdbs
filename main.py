#!/usr/bin/env python3

import os
import time
import json
import threading
import configparser
import datetime

from schema_scanner import *
from query_generator import *
from query_mutator_sequential import *
from query_generator_subqueries_with_graph import *

import graph_tool.all as gt
from testing import *

if __name__ == "__main__":
    print("=====GraphGenie=====")
    config = configparser.ConfigParser()
    config.read('graphgenie.ini')
    graphdb = config['default']['graphdb']
    language = config['default']['language']
    ip = config['default']['ip']
    port = int(config['default']['port'])
    username = config['default']['username']
    password = config['default']['password']

    
    from neo4j import GraphDatabase
    test = Neo4jTesting()
    graph_full = gt.Graph(directed=True)



    schema_scanner = Neo4jSchemaScanner(ip, port, username, password)
    node_labels, edge_labels, node_properties, connectivity_matrix, properties_types,graph_full = schema_scanner.scan(graph_full)
    
    print("CONNECTIVITY MATRIX:",connectivity_matrix)
    random_cypher_generator = RandomCypherGenerator_subqueries_with_graph(node_labels, edge_labels, node_properties, connectivity_matrix,properties_types,graph_full)
    cypher_query_mutator = CypherQueryMutatorSequential(node_labels, edge_labels, node_properties, connectivity_matrix, graph_full)
    
    # test.testing(random_cypher_generator, cypher_query_mutator)
    # random_cypher_generator.init()
    
    # for i in range(10):
    #     print("\n","="*100)
    #     base_query = random_cypher_generator.random_query_generator()
    #     print()
    #     print(base_query)
    #     print()
    #     print(random_cypher_generator.symbols)
    #     print(random_cypher_generator.node_symbols)
    #     print(random_cypher_generator.name_label_dict)
    #     print(random_cypher_generator.node_properties)
    #     print(f"property_to_test:{random_cypher_generator.property_to_test}")
    #     print(f"number of nested: {random_cypher_generator.number_nested_predicates}")
    #     input()
    
    # # for i in range(10):
    # #     equivalent_queries, equivalent_rules_eval = cypher_query_mutator.generate_equivalent_queries(base_query)
    # #     print(equivalent_queries)
    # #     print()
    # #     print(equivalent_rules_eval)
    # #     print("=================================")

