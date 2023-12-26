#!/usr/bin/env python3

import os
import time

from neo4j import GraphDatabase,unit_of_work
import numpy as np

import graph_tool.all as gt
# currently we only support neo4j
from tqdm import tqdm

class SchemaScanner:
    node_count = 0
    edge_count = 0
    node_labels = []
    edge_labels = []
    node_properties = {}
    connectivity_matrix = []

    def __init__(self, ip, port, username, password):
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        pass

    def scan(self):
        pass

    def print_schema_info(self):
        print("Node count: {}".format(self.node_count))
        print("Edge count: {}".format(self.edge_count))
        print("Node labels: {}".format(str(self.node_labels)))
        print("Edge labels: {}".format(str(self.edge_labels)))

    def print_connectivity(self):
        for i in self.node_labels:
            print(i[0], end=" ")
        print()
        for x in self.connectivity_matrix:
            for y in x:
                print(y, end=" ")
            print()

class Neo4jSchemaScanner(SchemaScanner):
    def neo4j_init(self):
        url = "bolt://{}:{}".format(self.ip, self.port)
        username = self.username
        password = self.password
        self.driver = GraphDatabase.driver(url, auth=(username, password),connection_timeout=60*10  )
        
    def scan(self, graph_full=None):
        if graph_full is None:
            graph_full = gt.Graph(directed=True)
        self.graph_full = graph_full

        self.neo4j_init()
        get_node_count_query = "MATCH (n) RETURN count(n)"
        self.node_count = (list(self.execute_query(get_node_count_query)[0].values())[0])
        get_edge_count_query = "MATCH ()-[n]-() RETURN count(n)"
        self.edge_count = (list(self.execute_query(get_edge_count_query)[0].values())[0])
        get_node_labels_query = """
            MATCH (n)
            WITH DISTINCT labels(n) AS labels
            UNWIND labels AS label
            RETURN DISTINCT label
            ORDER BY label;
        """
        query_result = self.execute_query(get_node_labels_query)
        for i in query_result:
            self.node_labels.append(i['label'])
        get_edge_labels_query = """
            MATCH ()-[n]-()
            WITH DISTINCT type(n) AS labels
            UNWIND labels AS label
            RETURN DISTINCT label
            ORDER BY label;
        """
        query_result = self.execute_query(get_edge_labels_query)
        for i in query_result:
            self.edge_labels.append(i['label'])
        self.print_schema_info()
        self.scan_properties()
        self.scan_keys_types()
        self.scan_connectivity()
        # self.scan_connectivity_matrix()
        self.scan_graph_fill()
        return self.node_labels, self.edge_labels, self.node_properties, self.connectivity_matrix, self.properties_types, self.graph_full

    def scan_properties(self):
        get_properties_query = """
            MATCH (n:{})
            WITH DISTINCT(keys(n)) as key_sets
            UNWIND(key_sets) as keys
            RETURN DISTINCT(keys) as key;
        """
        for each_node_label in self.node_labels:
            properties = []
            query_result = self.execute_query(get_properties_query.format(each_node_label))
            for i in query_result:
                properties.append(i['key'])
            self.node_properties[each_node_label] = properties

    def scan_connectivity(self):
        for each_node_label in self.node_labels:
            matrix_row = []
            for each_test_node_label in self.node_labels:
                if each_node_label==each_test_node_label:
                    matrix_row.append(0)
                else:
                    test_query = "MATCH (a:{})-->(b:{}) RETURN count(a)".format(
                        each_node_label,
                        each_test_node_label
                    )
                    query_result = (list(self.execute_query(test_query)[0].values())[0])
                    matrix_row.append(0 if query_result==0 else 1)
            self.connectivity_matrix.append(matrix_row)
        self.print_connectivity()

    @unit_of_work(timeout=60*1000)
    def execute_query(self, query):
        with self.driver.session() as session:
            query_result = session.execute_write(self._new_execute, query)
            return query_result
        
    def scan_keys_types(self):
        # Scans the DB like scan_properties but gets the type of each keys
        # Rolando
        get_properties_types_query = """
            MATCH (n:{node_label})
            WHERE n.{node_property} IS NOT NULL 
            RETURN Distinct(valueType(n.{node_property})) as {node_property} ;
        """
        print(f"each_node_label: {self.node_labels}")
        properties_types = {}
        for each_node_label in self.node_labels:
            for propertie in self.node_properties[each_node_label]:
                properties = []
                query_result = self.execute_query(get_properties_types_query.format(node_label=each_node_label,node_property=propertie))
                # print(each_node_label,query_result)
                
                for i in query_result:
                    properties_types.update(query_result[0]) 
        print(f"properties_types:{properties_types}")
        self.properties_types = properties_types
        
    def scan_connectivity_matrix(self):
        # Deprecated: This was old approach using a connectivity matrix
        print("CALCULATING REAL CONNECTIVITY")
        # We want to scan for each label the connection matrix.
        
        # Set an ID to each node:
        set_id_query = "match (n) set n.id = id(n)"
        self.execute_query(set_id_query)
        # Gather list of all ids
        get_all_ids_query = "match (n) Return n.id order BY n.id desc"
        all_ids = self.execute_query(get_all_ids_query)
        print("all_ids",all_ids[0:5])
        largest_id = all_ids[0]["n.id"]
        # print("largest_id",largest_id,type(largest_id))
        # input()
        # Init matrices:
        self.connectivity_nparray_per_edge_label_dict ={
            edge_label:np.zeros((largest_id+1,largest_id+1),dtype=np.uint32) for edge_label in self.edge_labels # +1 for testing with updates later. Dtype of uint32 for 0-4294967295
        }
        # fill matrices
        for edge_label in self.edge_labels:
            query= f"match (n)-[r:{edge_label}]->(n2) Return n.id,n2.id order BY n.id"
            result = self.execute_query(query=query)
            # print("RESULT:",result[0:5])
            matrix = self.connectivity_nparray_per_edge_label_dict[edge_label]
            for line in result:
                # print("LINE:",line)
                # input()
                nid,n2id = int(line["n.id"]), int(line["n2.id"])
                matrix[nid,n2id] +=1
            
                
    def scan_graph_fill(self):
        # Beginning to fill the graph
        print("===FILLING GRAPH (Can take a while)===")
        # Setting ids to each node
        print("Setting ids to each node")
        set_id_query = "match (n) set n.id = id(n)"
        self.execute_query(set_id_query)
        # Gather list of all ids
        print("Gather list of all ids")
        get_all_ids_query = "match (n) Return n.id order BY n.id desc"
        all_ids = self.execute_query(get_all_ids_query)
        print("all_ids",all_ids[0:5])
        largest_id = all_ids[0]["n.id"]
        self.graph_full_vertices = [self.graph_full.add_vertex() for _ in range(largest_id+1)] 
        self.graph_nodes_dicts = {}   
        self.graph_edges_dicts = {}
        #Properties
        node_properties = self.graph_full.new_vertex_property("object")
        edge_properties = self.graph_full.new_edge_property("object")
        self.graph_full.vp["properties"] = node_properties
        self.graph_full.ep["properties"] = edge_properties

        node_labels = self.graph_full.new_vertex_property("object")
        self.graph_full.vp["labels"] = node_labels
        #Query and add labels:
        query = "match(n) Return n.id,Labels(n)"
        labels = self.execute_query(query)
        for label in tqdm(labels):
            nid = int(label["n.id"])
            vertice = self.graph_full.vertex(nid)
            self.graph_full.vertex_properties["labels"][vertice]= label["Labels(n)"]

        # Adding edges
        print("Querying vertices and edges (Can take a while))") 
        query = "match (n)-[r]->(n2) Return n,r,n2 order BY n.id"
        relations = self.execute_query(query)
        print("Adding vertices and edges (with their properties) (Can take a while))")
        for relation in tqdm(relations):
            n1_id = int(relation["n"]["id"])
            n2_id = int(relation["n2"]["id"])
            vertice1 = self.graph_full.vertex(n1_id)
            vertice2 = self.graph_full.vertex(n2_id)
            edge =self.graph_full.add_edge(vertice1,vertice2)
            # Here we can add properties to the edges:
            # https://graph-tool.skewed.de/static/doc/quickstart.html#sec-property-maps
            self.graph_full.vertex_properties["properties"][vertice1]= relation["n"]
            self.graph_full.vertex_properties["properties"][vertice2]= relation["n2"]
            self.graph_full.edge_properties["properties"][edge] = relation["r"][1]



            

        print("Graph filled")
        print("===============")


        
                
    def scan_graph_fill_edges(self):
        pass
    
            
        

    @staticmethod
    @unit_of_work(timeout=60*1000)
    def _new_execute(tx, query):
        query_result = -1
        query_execute = tx.run(query)
        query_data = query_execute.data()
        return query_data
