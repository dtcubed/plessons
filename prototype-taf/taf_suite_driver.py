#!/usr/bin/python
#############################################################################

#####
# TAF modules.
#####
import taf_program_driver
import taf_rdbms
import taf_utility

import logging
import multiprocessing
import xml.etree.ElementTree as ET

from sys import argv, exit
from time import sleep 

#############################################################################
def declare_new_suite(database, name, version, production, stop_on_failure):

    #####
    # Get a sufficiently random string for reverse lookup purposes.
    #####
    a_random_string = taf_utility.get_random_string()

    #####
    # Acquire the current machine name if possible. If not, 'unavailable' 
    # will be the result.
    #####
    node_name = taf_utility.get_machine_name()
    
    #####
    # Acquire the current logged-in user possible. If not, 'unavailable' 
    # will be the result.
    #####
    current_user = taf_utility.get_current_user()
    
    #####
    # Now, create a new suite record.
    #####
    taf_rdbms.insert_suite(database, name, version, production, stop_on_failure, a_random_string, node_name, current_user)
   
    #####
    # Conduct a reverse-lookup and return the primary key of the suite.
    #####
    suite_id = taf_rdbms.lookup_suite_id(database, a_random_string, node_name, current_user)

    return suite_id

#############################################################################
def load_new_program(database, suite_id, suite_phase, program_node):

    arguments = program_node.find('arguments').text
    name = program_node.find('name').text
    sequential = program_node.find('sequential').text
    timeout_setup = program_node.find('timeout_setup').text
    timeout_main = program_node.find('timeout_main').text
    timeout_cleanup = program_node.find('timeout_cleanup').text

    taf_rdbms.insert_program(database, suite_id, suite_phase, sequential, timeout_setup, timeout_main, timeout_cleanup, name, arguments)

#############################################################################
def print_program_name(program_node):

    arguments = program_node.find('arguments').text
    name = program_node.find('name').text
    sequential = program_node.find('sequential').text
    timeout_setup = program_node.find('timeout_setup').text
    timeout_main = program_node.find('timeout_main').text
    timeout_cleanup = program_node.find('timeout_cleanup').text

    my_str  = "Name: [" + name + "] " + "Arguments: [" + arguments + "] "
    my_str += "Sequential: [" + sequential + "] " + "Setup Timeout: [" + timeout_setup + "]  "
    my_str += "Main Timeout: [" + timeout_main + "] Cleanup Timeout: [" + timeout_cleanup + "]"
    logging.debug(my_str)

#############################################################################
def program_driver_wrapper(database, program_id, mp_lock):

    test_program_driver = taf_program_driver.taf_program_driver(database, program_id)
    test_program_driver.run()
    mp_lock.acquire()
    mp_lock.release()

#############################################################################
def wait_for_active_children_to_complete():

    while multiprocessing.active_children():

        logging.info('Waiting for active children to complete.')
        sleep(2)
    
#############################################################################
## test_suite_driver ########################################################
#############################################################################
def test_suite_driver(database, suite_file_name):

    #####
    # Open the Suite file (XML), parse the contents, close the Suite file.
    #####
    suite_file_fd = file(suite_file_name, 'r')
    tree = ET.parse(suite_file_fd)
    suite_file_fd.close()
    
    root = tree.getroot()

    suite_name = root.find('./name[1]').text
    suite_production = root.find('./production[1]').text
    suite_stop_on_failure = root.find('./stop_on_failure[1]').text
    suite_version = root.find('./version[1]').text

    my_str  = "Suite Name: [" + suite_name + "] " + "Suite Version: [" + suite_version + "] "
    my_str += "Production: [" + suite_production + "] " + "Stop On Failure: [" + suite_stop_on_failure + "]"
    logging.debug(my_str)

    suite_id = declare_new_suite(database, suite_name, suite_version, suite_production, suite_stop_on_failure)

    my_str  = "Suite Id Returned: [" + str(suite_id) + "]"
    logging.debug(my_str)

    taf_rdbms.suite_start(database, str(suite_id))

    #########################################################################
    ######################## Load Phase #####################################
    #########################################################################
    taf_rdbms.suite_load_start(database, str(suite_id))

    for program_node in root.findall('./setup/program[1]'):
        print_program_name(program_node)
        load_new_program(database, str(suite_id), 'S', program_node)

    for program_node in root.findall('./main/program'):
        print_program_name(program_node)
        load_new_program(database, str(suite_id), 'M', program_node)

    for program_node in root.findall('./cleanup/program[1]'):
        print_program_name(program_node)
        load_new_program(database, str(suite_id), 'C', program_node)

    taf_rdbms.suite_load_end(database, str(suite_id))

    #########################################################################
    ######################## Setup Phase ####################################
    #########################################################################
    taf_rdbms.suite_setup_start(database, str(suite_id))

    setup_programs = taf_rdbms.lookup_program_ids_by_suite_id_and_suite_phase(database, str(suite_id), 'S')

    if setup_programs[0] != 'NONE':

        for program_id in setup_programs:

            test_program_driver = taf_program_driver.taf_program_driver(database, program_id)
            test_program_driver.run()

        suite_phase_status = taf_rdbms.determine_suite_phase_status(database, str(suite_id), 'S')
        taf_rdbms.update_suite_status(database, str(suite_id), 'S', 'setup_status', suite_phase_status)

    taf_rdbms.suite_setup_end(database, str(suite_id))
    #########################################################################
    ######################## Main Phase #####################################
    #########################################################################
    suite_setup_status = taf_rdbms.lookup_suite_status(database, str(suite_id), 'setup_status')

    #####
    # At the Test Suite level, we will only execute the MAIN phase if (and only if) the SETUP phase is a PASS
    # If this is not the case, the MAIN phase will be bypassed.
    #####
    if suite_setup_status == 'P' :

        taf_rdbms.suite_main_start(database, str(suite_id))

        main_programs = taf_rdbms.lookup_program_ids_by_suite_id_and_suite_phase(database, str(suite_id), 'M')

        if main_programs[0] != 'NONE':

            mp_lock = multiprocessing.Lock()

            #####
            # Implement parallelism for Test Programs here.
            #####
            for program_id in main_programs:

                is_program_sequential = taf_rdbms.lookup_program_sequential_by_program_id(database, program_id)

                #####
                # If the current Test Program is "sequential".
                #####
                if is_program_sequential == '1' :

                    msg  = 'MAIN program id: [' + str(program_id) + '] is SEQUENTIAL so, wait for children before and after'
                    logging.info(msg)
                    #####
                    # Wait for any/all child processes to complete before hand.
                    #####
                    wait_for_active_children_to_complete()

                    multiprocessing.Process(target=program_driver_wrapper, args=(database, program_id, mp_lock)).start()

                    #####
                    # Wait for any/all child processes to complete afterwards.
                    #####
                    wait_for_active_children_to_complete()

                else:

                    msg  = 'MAIN program id: [' + str(program_id) + '] is NON-SEQUENTIAL so, just invoke it'
                    logging.info(msg)
                    multiprocessing.Process(target=program_driver_wrapper, args=(database, program_id, mp_lock)).start()

               
            #####
            # Wait for all active child processes to complete before determining statuses.
            #####
            wait_for_active_children_to_complete()

            suite_phase_status = taf_rdbms.determine_suite_phase_status(database, str(suite_id), 'M')
            taf_rdbms.update_suite_status(database, str(suite_id), 'M', 'main_status', suite_phase_status)

        else:

            err_msg  = "Oops. No MAIN programs." 
            raise Exception(err_msg)

        taf_rdbms.suite_main_end(database, str(suite_id))

    #########################################################################
    ######################## Cleanup Phase ##################################
    #########################################################################
    suite_main_status = taf_rdbms.lookup_suite_status(database, str(suite_id), 'main_status')

    #####
    # We will bypass the CLEANUP phase if Stop On Failure (SOF) is TRUE AND we have a failure
    # perceived from either the SETUP or MAIN phases.
    #####
    if not ((suite_stop_on_failure == '1') and (suite_setup_status == 'F' or suite_main_status == 'F')) :

        taf_rdbms.suite_cleanup_start(database, str(suite_id))

        cleanup_programs = taf_rdbms.lookup_program_ids_by_suite_id_and_suite_phase(database, str(suite_id), 'C')

        if cleanup_programs[0] != 'NONE':

            for program_id in cleanup_programs:

                test_program_driver = taf_program_driver.taf_program_driver(database, program_id)
                test_program_driver.run()

            suite_phase_status = taf_rdbms.determine_suite_phase_status(database, str(suite_id), 'C')
            taf_rdbms.update_suite_status(database, str(suite_id), 'C', 'cleanup_status', suite_phase_status)

        taf_rdbms.suite_cleanup_end(database, str(suite_id))

    #########################################################################
    ######################## Final, Overall Status for Test Suite ###########
    #########################################################################
    suite_cleanup_status = taf_rdbms.lookup_suite_status(database, str(suite_id), 'cleanup_status')

    #####
    # Since the Cleanup phase may have been bypassed, look up the current suite phase to pass it along.
    #####
    current_suite_phase = taf_rdbms.lookup_suite_phase_by_suite_id(database, str(suite_id))
    
    if (suite_setup_status == 'P' and suite_main_status == 'P' and suite_cleanup_status == 'P') :

        taf_rdbms.update_suite_status(database, str(suite_id), current_suite_phase, 'overall_status', 'P')

    else:

        taf_rdbms.update_suite_status(database, str(suite_id), current_suite_phase, 'overall_status', 'F')

    #####
    # Reverse lookup of final, overall status for this Suite.
    #####
    suite_overall_status = taf_rdbms.lookup_suite_status(database, str(suite_id), 'overall_status')

    my_str = 'Overall Status For Suite: [' + str(suite_id) + '] is: [' + suite_overall_status + ']'
    logging.info(my_str)

    taf_rdbms.suite_end(database, str(suite_id))

#############################################################################
## main #####################################################################
#############################################################################
if __name__ == '__main__':

    #####
    # Setup logging to a file using basicConfig.
    # Levels from least to most severe: DEBUG, INFO, WARNING, ERROR, CRITICAL.
    #####
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y%m%d-%H%M%S',
                        filename='taf-log-file.txt', filemode='w', level=logging.INFO)

    #####
    # Create a console handler.
    #####
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    #####
    # Create a console handler formatter and set the console handler to use it.
    #####
    ch_formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y%m%d-%H%M%S')
    ch.setFormatter(ch_formatter)

    #####
    # Add the console handler to the root logger. Our log messages should now be
    # routed to two(2) places using the same formats.
    ####
    logging.getLogger('').addHandler(ch)

    taf_database = 'TAF.db'

    taf_rdbms.create_db_if_necessary(taf_database)

    suite_file_name = argv[1]

    test_suite_driver(taf_database, suite_file_name)

    exit(0)

#############################################################################
####################### EOF #################################################
#############################################################################
