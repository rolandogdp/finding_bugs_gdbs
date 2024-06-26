import os
import time
import json
import threading
import configparser
import datetime
from neo4j import GraphDatabase

class Testing:
    def init_testing_configs(self):
        config = configparser.ConfigParser()
        config.read('graphgenie.ini')
        self.threads = []
        self.prev_results = []
        self.bug_rules_eval = [0, 0, 0]
        self.variant = int(config['testing_configs']['variant'])
        self.multi_threading = int(config['testing_configs']['multi_threading'])
        self.perf_issue = int(config['testing_configs']['perf_issue'])
        self.threshold = float(config['testing_configs']['threshold'])
        self.variant_threshold = float(config['testing_configs']['variant_threshold'])
        self.minimum_test_ms = float(config['testing_configs']['minimum_test_ms'])
        self.ip = config['default']['ip']
        self.port = int(config['default']['port'])
        self.username = config['default']['username']
        self.password = config['default']['password']
        self.logpath = config['testing_configs']['logpath']
        self.bug_logpath = config['testing_configs']['bug_logpath']
        self.exception_logpath = config['testing_configs']['exception_logpath']
        self.min_save_log_size = int(config['testing_configs']['min_save_log_size'])
        self.max_testing_query_num = int(config['testing_configs']['max_testing_query_num'])
        self.testing_times = int(config['testing_configs']['testing_times'])
        self.statistics = int(config['testing_configs']['statistics'])
        self.logging_stop = int(config['testing_configs']['logging_stop'])

    def init_log(self):
        if os.path.exists(self.logpath) and os.path.getsize(self.logpath)>self.min_save_log_size:
            os.system("mv {} ./logs/testing.log-{}".format(self.logpath, str(time.time())))
        if os.path.exists(self.bug_logpath):
            os.system("mv {} ./logs/buggy.log-{}".format(self.bug_logpath, str(time.time())))
        if os.path.exists(self.exception_logpath):
            os.system("mv {} ./logs/exception.log-{}".format(self.exception_logpath, str(time.time())))

    # TODO: multi-thread sync

    def log(self, text):
        #TODO: Return this to normal
        print(text)
        # f = open(self.logpath, "a+")
        # f.write(text)
        # f.close()

    def bug_log(self, text):
        f = open(self.bug_logpath, "a+")
        f.write(text)
        f.close()

    def except_log(self, text):
        if "imeout" in text:
            return
        f = open(self.exception_logpath, "a+")
        f.write(text)
        f.close()

    def print_testing_results(self):
        self.log("number of tested base queries: {}\n".format(self.executed_base_query_num))
        self.log("number of detected inconsistent pair: {}\n".format(self.detected_bug_num))

    def time_checking(self, base_time, testing_time):
        diff = max(base_time, testing_time)/min(base_time, testing_time)
        if diff>self.threshold and testing_time>50.0 and testing_time<base_time:
            self.log("[***** Potential Performance Bug: DIFF={:.2f}times *****]\n".format(diff))
            self.bug_rules_eval[0] += self.current_rules_eval[0]
            self.bug_rules_eval[1] += self.current_rules_eval[1]
            self.bug_rules_eval[2] += self.current_rules_eval[2]
            return 1
        return 0

    def variant_time_checking(self, base_time, light_time):
        diff = max(light_time, base_time)/min(light_time, base_time)
        if diff>self.variant_threshold and light_time>base_time:
            self.log("[***** Potential Performance Bug: DIFF={:.2f}times *****]\n".format(diff))
            return 1
        return 0

    def result_checking(self, base_result, test_result, base_query, test_query):
        buggy = type(base_result)!=type(test_result) or base_result!=test_result
        if buggy and "count" in test_query:
            if base_result==None or test_result==None:
                self.log("[***** None Check *****]\n")
            else:
                # remove reduplicate bugs in one testing case
                if test_result in self.prev_results:
                    return
                self.detected_bug_num += 1
                self.bug_rules_eval[0] += self.current_rules_eval[0]
                self.bug_rules_eval[1] += self.current_rules_eval[1]
                self.bug_rules_eval[2] += self.current_rules_eval[2]
                self.log("[***** No.{} Potential Logic Bug: {} {} *****]\n".format(self.detected_bug_num, base_result, test_result))
                self.bug_log("{}\n[***** No.{} Potential Logic Bug:\n\tbase_query={}\n\ttest_query={}\n\tbase_result={}\ttest_result={}\n*****]\n".format(datetime.datetime.now(), self.detected_bug_num, base_query, test_query, base_result, test_result))
                self.prev_results.append(test_result)


    def performance_verification(self, base_query, testing_query, version):
        base_dbhits = self.execute_ret_dbhits(version, base_query)
        testing_dbhits = self.execute_ret_dbhits(version, testing_query)

    # function for multi-threading
    # Note: db may have sync issue => result inconsistency 
    def eq_testing(self, base_result, base_time, base_query, eq_query):
        eq_query_result, eq_query_time = self.execute_ret_result_time(eq_query, log_str="[Equivalent]")
        if base_result!=None or eq_query_result!=None:
            self.result_checking(base_result, eq_query_result, base_query, eq_query)
        if self.perf_issue==1 and base_time>self.minimum_test_ms:
            self.time_checking(base_time, eq_query_time)

    # only for testing single version single instance, no index testing
    def testing(self, random_cypher_generator, cypher_query_mutator):
        for i in range(self.testing_times):
            print("{} round testing".format(i+1))
            self.current_rules_eval = [0, 0, 0]
            self.executed_allquery = 0
            self.executed_query_num = 0
            self.executed_base_query_num = 0
            self.detected_bug_num = 0
            random_cypher_generator.init()
            while True:
                if self.executed_base_query_num % self.logging_stop == 0:
                    self.log("{} executed. Rules evaluation statitics: {}\n".format(self.executed_base_query_num, str(self.bug_rules_eval)))
                self.prev_results.clear()
                base_query = random_cypher_generator.random_query_generator()
                self.print_testing_results()
                self.executed_base_query_num += 1
                self.log("=================================\n")
                self.log("No.{} Base Query: {} \n".format(self.executed_base_query_num, base_query))
                base_query_result = -1
                base_query_time = -1
                base_query_result, base_query_time = self.execute_ret_result_time(query=base_query, log_str="[BASE QUERY]")
                equivalent_queries_transactions_create,equivalent_queries_transaction_match,equivalent_queries_transactions_delete  = cypher_query_mutator.generate_equivalent_queries(base_query)

                if base_query_time>0:
                    #Sequentially execute equivalent queries
                    #Preparation:
                    for create_query in equivalent_queries_transactions_create:
                        self.execute_no_log(create_query)
                    # Actual query
                    eq_query_result, eq_query_time = self.execute_ret_result_time(query=equivalent_queries_transaction_match, log_str="[Equivalent]")

                    self.result_checking(base_query_result, eq_query_result, base_query, equivalent_queries_transaction_match)
                    if self.perf_issue==1 and base_query_time>self.minimum_test_ms:
                        self.time_checking(base_query_time, eq_query_time)
                    
                    #Clean up
                    for delete_query in equivalent_queries_transactions_delete:
                        self.execute_no_log(delete_query)

                a = input("Waiting for next round testing...")
                if a=="q":
                    break


class Neo4jTesting(Testing):
    # Neo4j Config
    def __init__(self):
        self.init_testing_configs()
        self.start_time = time.time()
        if os.path.exists(self.logpath):
            self.init_log()
        self.log("***** Testing Neo4j *****\n")
        self.log("\tmulti_threading={}\n".format(self.multi_threading))
        self.log("\tvariant={}\n".format(self.variant))
        self.bolt_uri = "bolt://{}:{}".format(self.ip, self.port)
        self.driver = GraphDatabase.driver(self.bolt_uri, auth=(self.username, self.password))

    def execute_no_log(self, query):
        driver = self.driver
        query_result = None
        query_time = -1
        with driver.session() as session:
            try:
                # if possible, clear cache first
                clear_query = "CALL db.clearQueryCaches();"
                session.execute_write(self._new_execute, clear_query)
                print(f"Query:{query}\n")
                query_result, query_time = session.execute_write(self._new_execute, query)
            except Exception as e:
                print('\nQuery:{}\nInfo:{}\n'.format(query, str(e)))
                # self.except_log('\nQuery:{}\nInfo:{}\n'.format(query, str(e)))
                return None, -1
        return query_result, query_time
    
    # log/return result+time
    def execute_ret_result_time(self, query, log_str):
        driver = self.driver
        query_result = None
        query_time = -1
        with driver.session() as session:
            try:
                # if possible, clear cache first
                clear_query = "CALL db.clearQueryCaches();"
                session.execute_write(self._new_execute, clear_query)
                if "TRANSACTIONS" in query: # This is a quick patch to handle call in transactions
                    run_query= session.run(query)
                    print(f"Ran query with run: {run_query}\n")
                    query_result = run_query.values()
                    query_time = 0 # ??
                else:
                    query_result, query_time = session.execute_write(self._new_execute, query)
            except Exception as e:
                print('\nQuery:{}\nInfo:{}\n'.format(query, str(e)))
                # self.except_log('\nQuery:{}\nInfo:{}\n'.format(query, str(e)))
                return None, -1
            self.executed_allquery += 1
            self.log("No.{} {} Query=\"{}\"\n\tQuery Result={}\n\tQuery Time={}\n".format(self.executed_allquery, log_str, query, query_result, query_time))
        self.executed_query_num += 1
        return query_result, query_time

    @staticmethod
    def _new_execute(tx, query):
        ## clear cache first
        clear_query = "CALL db.clearQueryCaches();"
        tx.run(clear_query)
        query_result = -1
        query_time = -1
        query_execute = tx.run(query)
        query_data = query_execute.data()
        print(f"Query:{query}\n","Queried data:",query_data)
        if len(query_data)==0:
            query_result = None
        else:
            query_result = (list(query_data[0].values())[0])
        query_time = query_execute.consume().result_available_after + query_execute.consume().result_consumed_after
        return query_result, query_time

