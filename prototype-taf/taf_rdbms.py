#!/usr/bin/python
#############################################################################
import os.path

#####
# TAF modules.
#####
import taf_utility

import logging

#####
# From the ActiveState 2.7.5 documentation, it is pointed out that the 
# Python "re" module provides Perl style regular expressions. Earlier 
# versions of Python came with the "regex" module (Emacs style regexps)
# and that was removed from Python in 2.5.
#####
import re

from sqlite3 import dbapi2 as sqlite

from time import time

#############################################################################
def create_db_if_necessary(db_name):

    #####
    # Create Database if it doesn't alreay exist.
    #####
    if not os.path.isfile(db_name):

        #####
        # The UNIQUE INDEX below assures that each Test Program must use UNIQUE
        # "testware_program_key"(s) across all of the three (3) pertinent phases
        # (Setup/Main/Cleanup).
        #####
        unique_index_001_sql  = 'CREATE UNIQUE INDEX index001 ON assertion (program_id, testware_program_key) '

        print 'Creating Database: [' + db_name + ']'
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(create_table_sql('suite'))
        cursor.execute(create_table_sql('program'))
        cursor.execute(create_table_sql('assertion'))
        cursor.execute(create_table_sql('info'))
        print 'Executing: [' + unique_index_001_sql + ']'
        cursor.execute(unique_index_001_sql)
        connection.commit()
        connection.close()
        #####
        # Insert global information into the one record 'info' table.
        #####
        insert_info(db_name, 'TAF Prototype',  '20131213', '0', '0', '0', '1')

    else:    

        print 'Database already exists: [' + db_name + ']'
        return

#############################################################################
def create_table_sql(table_name):

    if table_name == 'suite': 

        sql =  'CREATE TABLE suite'
        sql += '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
        sql += 'name TEXT NOT NULL, '
        sql += 'version TEXT NOT NULL, '
        sql += 'phase TEXT NOT NULL, '
        sql += 'overall_status TEXT NOT NULL, '
        sql += 'setup_status TEXT NOT NULL, '
        sql += 'main_status TEXT NOT NULL, '
        sql += 'cleanup_status TEXT NOT NULL, '
        sql += 'production TEXT NOT NULL, '
        sql += 'stop_on_failure TEXT NOT NULL, '
        sql += 'suite_start_sse TEXT NOT NULL, '
        sql += 'load_start_sse TEXT NOT NULL, '
        sql += 'load_end_sse TEXT NOT NULL, '
        sql += 'setup_start_sse TEXT NOT NULL, '
        sql += 'setup_end_sse TEXT NOT NULL, '
        sql += 'main_start_sse TEXT NOT NULL, '
        sql += 'main_end_sse TEXT NOT NULL, '
        sql += 'cleanup_start_sse TEXT NOT NULL, '
        sql += 'cleanup_end_sse TEXT NOT NULL, '
        sql += 'suite_end_sse TEXT NOT NULL, '
        sql += 'current_user TEXT NOT NULL, '
        sql += 'machine_name TEXT NOT NULL, '
        sql += 'randomly_generated TEXT NOT NULL)'

    elif table_name == 'program': 

        sql =  'CREATE TABLE program'
        sql += '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
        sql += 'suite_id INTEGER NOT NULL, '
        sql += 'suite_phase TEXT NOT NULL, '
        sql += 'phase TEXT NOT NULL, '
        sql += 'overall_status TEXT NOT NULL, '
        sql += 'ident_status TEXT NOT NULL, '
        sql += 'setup_status TEXT NOT NULL, '
        sql += 'main_status TEXT NOT NULL, '
        sql += 'cleanup_status TEXT NOT NULL, '
        sql += 'ident_name TEXT NOT NULL, '
        sql += 'ident_version TEXT NOT NULL, '
        sql += 'ident_start_sse TEXT NOT NULL, '
        sql += 'ident_end_sse TEXT NOT NULL, '
        sql += 'setup_start_sse TEXT NOT NULL, '
        sql += 'setup_end_sse TEXT NOT NULL, '
        sql += 'main_start_sse TEXT NOT NULL, '
        sql += 'main_end_sse TEXT NOT NULL, '
        sql += 'cleanup_start_sse TEXT NOT NULL, '
        sql += 'cleanup_end_sse TEXT NOT NULL, '
        sql += 'sequential TEXT NOT NULL, '
        sql += 'ident_timeout TEXT NOT NULL, '
        sql += 'setup_timeout TEXT NOT NULL, '
        sql += 'main_timeout TEXT NOT NULL, '
        sql += 'cleanup_timeout TEXT NOT NULL, '
        sql += 'name TEXT NOT NULL, '
        sql += 'arguments TEXT NOT NULL, '
        sql += 'randomly_generated TEXT NOT NULL)'

    elif table_name == 'assertion': 

        sql =  'CREATE TABLE assertion'
        sql += '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
        sql += 'program_id INTEGER NOT NULL, '
        sql += 'program_phase TEXT NOT NULL, '
        sql += 'testware_program_key TEXT NOT NULL, '
        sql += 'status TEXT NOT NULL, '
        sql += 'description TEXT NOT NULL, '
        sql += 'requirements_trace TEXT NOT NULL, '
        sql += 'declaration_sse TEXT NOT NULL, '
        sql += 'final_status_sse TEXT NOT NULL)'

    elif table_name == 'info': 

        sql =  'CREATE TABLE info '
        sql += '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
        sql += 'name TEXT NOT NULL, '
        sql += 'release_yyyymmdd TEXT NOT NULL, '
        sql += 'version_first TEXT NOT NULL, '
        sql += 'version_second TEXT NOT NULL, '
        sql += 'version_third TEXT NOT NULL, '
        sql += 'version_fourth TEXT NOT NULL)'

    else:

        sql = 'INVALID'

    logging.debug(sql)

    return sql

#############################################################################
def determine_program_phase_status(db_name, program_id, program_phase):

    sql_passed_assertions  = 'SELECT id FROM assertion '
    sql_passed_assertions += 'WHERE program_id = "' + program_id + '" '
    sql_passed_assertions += 'AND program_phase = "' + program_phase + '" '
    sql_passed_assertions += 'AND status = "P" '
    
    sql_non_passed_assertions  = 'SELECT id FROM assertion '
    sql_non_passed_assertions += 'WHERE program_id = "' + program_id + '" '
    sql_non_passed_assertions += 'AND program_phase = "' + program_phase + '" '
    sql_non_passed_assertions += 'AND status != "P" '

    logging.debug("SQL for PASSED Assertions    : [" + sql_passed_assertions + "]")

    passed_count = record_count_from_select(db_name, sql_passed_assertions)

    logging.debug("SQL for non-PASSED Assertions: [" + sql_non_passed_assertions + "]")

    non_passed_count = record_count_from_select(db_name, sql_non_passed_assertions)

    if non_passed_count == 0 and passed_count >= 1:

        return 'P'

    else:

        return 'F'

#############################################################################
def determine_suite_phase_status(db_name, suite_id, suite_phase):

    sql_passed_programs  = 'SELECT id FROM program '
    sql_passed_programs += 'WHERE suite_id = "' + suite_id + '" '
    sql_passed_programs += 'AND suite_phase = "' + suite_phase + '" '
    sql_passed_programs += 'AND overall_status = "P" '
    
    sql_non_passed_programs  = 'SELECT id FROM program '
    sql_non_passed_programs += 'WHERE suite_id = "' + suite_id + '" '
    sql_non_passed_programs += 'AND suite_phase = "' + suite_phase + '" '
    sql_non_passed_programs += 'AND overall_status != "P" '
    
    logging.debug("SQL for PASSED Programs     : [" + sql_passed_programs + "]")

    passed_count = record_count_from_select(db_name, sql_passed_programs)

    logging.debug("SQL for non-PASSED Programs : [" + sql_non_passed_programs + "]")

    non_passed_count = record_count_from_select(db_name, sql_non_passed_programs)

    if non_passed_count == 0 and passed_count >= 1:

        return 'P'

    else:

        return 'F'

#############################################################################
def insert_assertion(db_name, program_id, program_phase, testware_program_key, status, description, requirements_trace):

    if os.path.isfile(db_name):

        p01 = program_id
        p02 = program_phase
        p03 = testware_program_key
        p04 = status
        p05 = description
        p06 = requirements_trace

        declaration_sse = str(int(time()))

        if status == 'P':

            final_status_sse = declaration_sse

        elif status == 'F':

            final_status_sse = declaration_sse

        elif status == 'X':

            final_status_sse = '0'

        else:

            err_msg = 'There are only three valid assertion status values: [P, F, X]'
            logging.error(err_msg)
            raise Exception(err_msg)

        #####################################################################################
        # Start "guard" - against duplicate testware program keys.
        # TODO: remove this guard when a UNIQUE INDEX has been created in the assertion table
        # for the combined key (program_id + testware_program_key).
        #####################################################################################
        sql_guard  = 'SELECT id FROM assertion '
        sql_guard += 'WHERE program_id = "' + program_id + '" '
        sql_guard += 'AND testware_program_key = "' + testware_program_key + '" '

        prior_assertions_with_same_testware_key = record_count_from_select(db_name, sql_guard)

        if prior_assertions_with_same_testware_key != 0 :

            err_msg  = 'DUPLICATE testware key used in same program. This SQL: [' + sql_guard + '] '
            err_msg += 'should return 0 records and [' + str(prior_assertions_with_same_testware_key)
            err_msg += '] were returned. '
            logging.error(err_msg)
            raise Exception(err_msg)

        #####################################################################################
        # End "guard" - against duplicate testware program keys.
        #####################################################################################

        sql =  'INSERT INTO assertion VALUES '
        sql += '(null, ?, ?, ?, ?, ?, ?, ?, ?)'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql, (p01, p02, p03, p04, p05, p06, declaration_sse, final_status_sse))
        connection.commit()
        connection.close()

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def insert_info(db_name, name, release_yyyymmdd, version_first, version_second, version_third, version_fourth):

    if os.path.isfile(db_name):

        #####
        # This routine/method should only be called once. Ideally, right after DB creation.
        # The 'info' table is just intended to be a one(1) record table used for global versioning
        # information.
        #####
        sql =  'SELECT name FROM info'
        record_count = record_count_from_select(db_name, sql)
        if record_count != 0:

            err_msg = 'Expecting 0 records to be returned from: [' + sql + '] and got: [' + str(record_count) + ']'
            logging.error(err_msg)
            raise Exception(err_msg)

        p01 = name
        p02 = release_yyyymmdd
        p03 = version_first
        p04 = version_second
        p05 = version_third
        p06 = version_fourth

        sql =  'INSERT INTO info VALUES '
        sql += '(null, ?, ?, ?, ?, ?, ?)'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql, (p01, p02, p03, p04, p05, p06))
        connection.commit()
        connection.close()

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def insert_program(db_name, suite_id, suite_phase, sequential, timeout_setup, timeout_main, timeout_cleanup, name, arguments):

    if os.path.isfile(db_name):

        p01 = suite_id
        p02 = suite_phase
        p19 = sequential 
        p21 = timeout_setup
        p22 = timeout_main
        p23 = timeout_cleanup
        p24 = name
        p25 = arguments
        p26 = taf_utility.get_random_string()

        sql =  'INSERT INTO program VALUES '
        sql += '(null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql, (p01, p02, 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X', '0', '0', '0', '0', '0', '0', '0', '0', p19, '10', p21, p22, p23, p24, p25, p26))
        connection.commit()
        connection.close()

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def insert_suite(db_name, name, version, production, stop_on_failure, randomly_generated, machine_name, current_user):
   
    if os.path.isfile(db_name):

        p01 = name
        p02 = version
        p08 = production
        p09 = stop_on_failure
        p20 = current_user 
        p21 = machine_name
        p22 = randomly_generated 

        sql =  'INSERT INTO suite VALUES '
        sql += '(null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql, (p01, p02, 'X', 'X', 'X', 'X', 'X', p08, p09, '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', p20, p21, p22))
        connection.commit()
        connection.close()

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

##############################################################################
def lookup_program_arguments_by_program_id(db_name, program_id):

    arguments = single_field_select_by_id(db_name, 'program', 'arguments', program_id)

    return(arguments)

##############################################################################
def lookup_program_cleanup_timeout_by_program_id(db_name, program_id):

    program_cleanup_timeout = single_field_select_by_id(db_name, 'program', 'cleanup_timeout', program_id)

    return(program_cleanup_timeout)

##############################################################################
def lookup_program_ident_timeout_by_program_id(db_name, program_id):

    program_ident_timeout = single_field_select_by_id(db_name, 'program', 'ident_timeout', program_id)

    return(program_ident_timeout)

##############################################################################
def lookup_program_ids_by_suite_id(db_name, suite_id):

    if os.path.isfile(db_name):

        program_id_list = []

        sql =  'SELECT id FROM program '
        sql += 'WHERE suite_id ="' + suite_id + '" '
        sql += 'ORDER BY id ASC'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)

        #####
        # Ok, we are going to be ultra-safe here and ensure that the SQL returns 
        # at least one record. If this isn't the case, we'll throw an Exception.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            program_id_list.append(str(row[0]))

        connection.close()

        if (record_counter < 1):

            err_msg  = "For suite_id: [" + suite_id + "] we expect as least one "
            err_msg += "program record and the counter shows: [" + str(record_counter) + "]"
            logging.error(err_msg)
            raise Exception(err_msg)

        return(program_id_list)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

##############################################################################
def lookup_program_ids_by_suite_id_and_suite_phase(db_name, suite_id, suite_phase):

    if os.path.isfile(db_name):

        program_id_list = []

        sql =  'SELECT id FROM program '
        sql += 'WHERE suite_id ="' + suite_id + '" AND suite_phase ="' + suite_phase + '" '
        sql += 'ORDER BY id ASC'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)

        #####
        # This SQL is not always expected to return one or more records.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            program_id_list.append(str(row[0]))

        connection.close()

        #####
        # If there are no records, just return a list with 'NONE' as first element.
        #####
        if (record_counter == 0):

            program_id_list.append('NONE')

        return(program_id_list)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

##############################################################################
def lookup_program_main_timeout_by_program_id(db_name, program_id):

    program_main_timeout = single_field_select_by_id(db_name, 'program', 'main_timeout', program_id)

    return(program_main_timeout)

##############################################################################
def lookup_program_name_by_program_id(db_name, program_id):

    program_name = single_field_select_by_id(db_name, 'program', 'name', program_id)

    return(program_name)

##############################################################################
def lookup_program_phase_by_program_id(db_name, program_id):

    program_phase = single_field_select_by_id(db_name, 'program', 'phase', program_id)

    return(program_phase)

##############################################################################
def lookup_program_randomly_generated_by_program_id(db_name, program_id):

    program_randomly_generated = single_field_select_by_id(db_name, 'program', 'randomly_generated', program_id)

    return(program_randomly_generated)

##############################################################################
def lookup_program_setup_timeout_by_program_id(db_name, program_id):

    program_setup_timeout = single_field_select_by_id(db_name, 'program', 'setup_timeout', program_id)

    return(program_setup_timeout)

#############################################################################
def lookup_program_sequential_by_program_id(db_name, program_id):

    program_sequential = single_field_select_by_id(db_name, 'program', 'sequential', program_id)

    if not (program_sequential == '0' or program_sequential == '1'):

        err_msg = 'The only valid values for the sequential field are [0, 1]'
        logging.error(err_msg)
        raise Exception(err_msg)

    return(program_sequential)

#############################################################################
def lookup_program_status(db_name, program_id, specific_status):

    #####
    # Check the "specific_status" parameter for validity right away.
    #####
    if specific_status == 'overall_status':

        pass

    elif specific_status == 'ident_status':

        pass

    elif specific_status == 'setup_status':

        pass

    elif specific_status == 'main_status':

        pass

    elif specific_status == 'cleanup_status':

        pass

    else:

        err_msg = 'Only [overall, ident, setup, main, cleanup] specific status types allowed'
        logging.error(err_msg)
        raise Exception(err_msg)

    program_status = single_field_select_by_id(db_name, 'program', specific_status, program_id)

    #####
    # Do a paranoid double-check to ensure that only valid status codes are returned. 
    #####
    if not (program_status == 'X' or program_status == 'P' or program_status == 'F'):

        err_msg = 'The only valid status codes are: [X, P, F]'
        logging.error(err_msg)
        raise Exception(err_msg)

    return(program_status)

##############################################################################
def lookup_program_suite_id_by_program_id(db_name, program_id):

    program_suite_id = single_field_select_by_id(db_name, 'program', 'suite_id', program_id)

    return(program_suite_id)

##############################################################################
def lookup_suite_id(db_name, randomly_generated, machine_name, current_user):

    if os.path.isfile(db_name):

        sql =  'SELECT id FROM suite '
        sql += 'WHERE randomly_generated ="' + randomly_generated + '" '
        sql += 'AND machine_name="' + machine_name + '" '
        sql += 'AND current_user="' + current_user + '"'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)
        #####
        # Ok, we are going to be ultra-safe/paranoid here and count to ensure
        # that the SQL only returns one record here. If this isn't the case,
        # we'll throw an Exception.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            suite_id = row[0]

        connection.close()

        if (record_counter != 1):

            err_msg  = "We expect exactly one record and counter shows: ["
            err_msg += str(record_counter) + "]"
            logging.error(err_msg)
            raise Exception(err_msg)

        return(suite_id)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

##############################################################################
def lookup_suite_phase_by_suite_id(db_name, suite_id):

    suite_phase = single_field_select_by_id(db_name, 'suite', 'phase', suite_id)

    return(suite_phase)

#############################################################################
def lookup_suite_status(db_name, suite_id, specific_status):

    #####
    # Check the "specific_status" parameter for validity right away.
    #####
    if specific_status == 'overall_status':

        pass

    elif specific_status == 'setup_status':

        pass

    elif specific_status == 'main_status':

        pass

    elif specific_status == 'cleanup_status':

        pass

    else:

        err_msg = 'Only [overall, setup, main, cleanup] specific status types allowed'
        logging.error(err_msg)
        raise Exception(err_msg)

    suite_status = single_field_select_by_id(db_name, 'suite', specific_status, suite_id)

    #####
    # Do a paranoid double-check to ensure that only valid status codes are returned. 
    #####
    if not (suite_status == 'X' or suite_status == 'P' or suite_status == 'F'):

        err_msg = 'The only valid status codes are: [X, P, F]'
        logging.error(err_msg)
        raise Exception(err_msg)

    return(suite_status)

##############################################################################
def lookup_suite_stop_on_failure_by_suite_id(db_name, suite_id):

    suite_stop_on_failure = single_field_select_by_id(db_name, 'suite', 'stop_on_failure', suite_id)

    return(suite_stop_on_failure)

#############################################################################
def process_taf_program_output_file(db_name, taf_file):

    assert_regexp   = re.compile(r'^ASSERT\|(\S+)\|(\S+)\|(.*)\|(.*)$')
    end_regexp      = re.compile(r'^(\S+)\|(\d+)\|(\S+)\|END$')
    identity_regexp = re.compile(r'^IDENTITY\|(\S+)\|(\S+)$')
    start_regexp    = re.compile(r'^(\S+)\|(\d+)\|(\S+)\|START$')
    update_regexp   = re.compile(r'^UPDATE\|(\S+)\|(\S+)$')

    logging.info('Processing TAF file: [' + taf_file + ']')

    with open(taf_file, 'r') as source_taf_file:

        rdr = iter(source_taf_file)

        first_line = next(rdr)

        first_line = taf_utility.strip_trailing_newlines(first_line)

        if start_regexp.match(first_line):
			        
            m = start_regexp.match(first_line)
            parsed_program_randomly_generated = m.group(1)
            parsed_program_id = m.group(2)
            parsed_program_phase = m.group(3)
			
            sql =  'SELECT id FROM program '
            sql += 'WHERE randomly_generated ="' + parsed_program_randomly_generated + '" '
            sql += 'AND phase="' + parsed_program_phase + '" '
            sql += 'AND id ="' + parsed_program_id + '"'
			
            logging.debug("SQL: [" + sql + "]")

            record_count = record_count_from_select(db_name, sql)
			
            if record_count != 1:
			
                err_msg = 'Expecting 1 record to be returned from: [' + sql + '] and got: [' + str(record_count) + ']'
                logging.error(err_msg)
                raise Exception(err_msg)

        else:
			
            err_msg = 'File: [' + taf_file + '] contains invalid first line: [' + first_line + ']'
            logging.error(err_msg)
            raise Exception(err_msg)

        line_counter = 1

        logging.info('Line Number: [' + str(line_counter) + '] [' + first_line + ']')

        #####
        # Finish processing the remainder of the lines.
        #####
        for current_line in rdr:

            current_line = taf_utility.strip_trailing_newlines(current_line)
            line_counter += 1

            logging.info('Line Number: [' + str(line_counter) + '] [' + current_line + ']')

            if identity_regexp.match(current_line):

                #print 'IDENTIFY matched!'
                my_str  = 'Program Id: [' + parsed_program_id + '] ' 
                my_str += 'Program Random: [' + parsed_program_randomly_generated + '] ' 
                my_str += 'Program Phase: [' + parsed_program_phase + '] ' 
                #print my_str

                m = identity_regexp.match(current_line)

                parsed_ident_name = m.group(1)
                parsed_ident_version = m.group(2)

                #print "Ident name: [" + parsed_ident_name + "]"
                #print "Ident version: [" + parsed_ident_version + "]"

                if line_counter != 2:

                    err_msg = 'This: [' + current_line + '] should only occur on line 2.'
                    logging.error(err_msg)
                    raise Exception(err_msg)

                if parsed_program_phase != 'I':

                    err_msg = 'This: [' + current_line + '] should only occur in the IDENT phase.'
                    logging.error(err_msg)
                    raise Exception(err_msg)

                single_field_update(db_name, 'program', 'ident_name', parsed_program_id, parsed_ident_name)
                single_field_update(db_name, 'program', 'ident_version', parsed_program_id, parsed_ident_version)

            elif assert_regexp.match(current_line):

                #print 'ASSERT matched!'
                my_str  = 'Program Id: [' + parsed_program_id + '] ' 
                my_str += 'Program Random: [' + parsed_program_randomly_generated + '] ' 
                my_str += 'Program Phase: [' + parsed_program_phase + '] ' 
                #print my_str

                m = assert_regexp.match(current_line)

                assertion_testware_program_key = m.group(1)
                assertion_status = m.group(2)
                assertion_description = m.group(3)
                assertion_requirements_trace = m.group(4)

                p02 = parsed_program_id
                p03 = parsed_program_phase

                insert_assertion(db_name, p02, p03, assertion_testware_program_key, assertion_status, assertion_description, assertion_requirements_trace)


            elif update_regexp.match(current_line):

                #print 'UPDATE matched!'
                my_str  = 'Program Id: [' + parsed_program_id + '] ' 
                my_str += 'Program Random: [' + parsed_program_randomly_generated + '] ' 
                my_str += 'Program Phase: [' + parsed_program_phase + '] ' 
                #print my_str

                m = update_regexp.match(current_line)

                assertion_testware_program_key = m.group(1)
                assertion_status = m.group(2)

                p02 = parsed_program_id
                p03 = parsed_program_phase

                update_assertion_status(db_name, p02, p03, assertion_testware_program_key, assertion_status)


            elif end_regexp.match(current_line):

                #print 'END matched!'
                m = end_regexp.match(current_line)
                end_parsed_program_randomly_generated = m.group(1)
                end_parsed_program_id = m.group(2)
                end_parsed_program_phase = m.group(3)
			
                sql =  'SELECT id FROM program '
                sql += 'WHERE randomly_generated ="' + end_parsed_program_randomly_generated + '" '
                sql += 'AND phase="' + end_parsed_program_phase + '" '
                sql += 'AND id ="' + end_parsed_program_id + '"'
			
                record_count = record_count_from_select(db_name, sql)
			
                if record_count != 1:
			
                    err_msg = 'Expecting 1 record to be returned from: [' + sql + '] and got: [' + str(record_count) + ']'
                    logging.error(err_msg)
                    raise Exception(err_msg)

                #####
                # Additional sanity checks that are possibly too extreme (e.g. redundant).
                #####
                if parsed_program_randomly_generated != end_parsed_program_randomly_generated:

                    err_msg  = 'Start program randomly generated: [' + parsed_program_randomly_generated + '] '
                    err_msg += 'is not equal to the end program randomly generated: [' + end_parsed_program_randomly_generated + '] '
                    logging.error(err_msg)
                    raise Exception(err_msg)
                    
                elif parsed_program_id != end_parsed_program_id:

                    err_msg  = 'Start program id: [' + parsed_program_id + '] '
                    err_msg += 'is not equal to the end program id: [' + end_parsed_program_id + '] '
                    logging.error(err_msg)
                    raise Exception(err_msg)

                elif parsed_program_phase != end_parsed_program_phase:

                    err_msg  = 'Start program phase: [' + parsed_program_phase + '] '
                    err_msg += 'is not equal to the end program phase: [' + end_parsed_program_phase + '] '
                    logging.error(err_msg)
                    raise Exception(err_msg)

            else:

                err_msg = 'File: [' + taf_file + '] contains invalid line: [' + current_line + ']'
                logging.error(err_msg)
                raise Exception(err_msg)

    source_taf_file.close()

#############################################################################
def program_cleanup_end(db_name, program_id):

    single_field_update(db_name, 'program', 'cleanup_end_sse', program_id, str(int(time())))

#############################################################################
def program_cleanup_start(db_name, program_id):

    single_field_update(db_name, 'program', 'cleanup_start_sse', program_id, str(int(time())))
    single_field_update(db_name, 'program', 'phase', program_id, 'C')

#############################################################################
def program_ident_end(db_name, program_id):

    single_field_update(db_name, 'program', 'ident_end_sse', program_id, str(int(time())))

#############################################################################
def program_ident_start(db_name, program_id):

    single_field_update(db_name, 'program', 'ident_start_sse', program_id, str(int(time())))
    single_field_update(db_name, 'program', 'phase', program_id, 'I')

#############################################################################
def program_main_end(db_name, program_id):

    single_field_update(db_name, 'program', 'main_end_sse', program_id, str(int(time())))

#############################################################################
def program_main_start(db_name, program_id):

    single_field_update(db_name, 'program', 'main_start_sse', program_id, str(int(time())))
    single_field_update(db_name, 'program', 'phase', program_id, 'M')

#############################################################################
def program_setup_end(db_name, program_id):

    single_field_update(db_name, 'program', 'setup_end_sse', program_id, str(int(time())))

#############################################################################
def program_setup_start(db_name, program_id):

    single_field_update(db_name, 'program', 'setup_start_sse', program_id, str(int(time())))
    single_field_update(db_name, 'program', 'phase', program_id, 'S')

#############################################################################
def record_count_from_select(db_name, sql_stmt):

    sql_select_stmt_regexp = re.compile(r'^\s*SELECT\s+')
    
    if os.path.isfile(db_name):

        #print "SQL: [" + sql_stmt + "]"
        #####
        # Ensure that the "supposed" SQL SELECT (read-only) statement really starts
        # with "SELECT".
        #####
        if sql_select_stmt_regexp.match(sql_stmt):
                
            connection = sqlite.connect(db_name)
            cursor = connection.cursor()
            cursor.execute(sql_stmt)

            #####
            # Now, count the number of records returned.
            #####
            record_counter = 0 

            for row in cursor:

                record_counter += 1

            connection.close()

            return(record_counter)

        else:

            err_msg  = 'This: [' + sql_stmt + '] does not start out with "SELECT" '
            logging.error(err_msg)
            raise Exception(err_msg)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def single_field_select_by_id(db_name, table_name, field_name, record_id):

    if os.path.isfile(db_name):

        sql =  'SELECT ' + field_name + ' FROM ' + table_name + ' '
        sql += 'WHERE id ="' + str(record_id) + '" '
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)

        #####
        # In this routine, we expect exactly one (1) record to fit the selection
        # criteria.  If this isn't the case, we'll throw an Exception.
        #####
        record_counter = 0 

        for row in cursor:
            record_counter += 1
            field = row[0]

        connection.close()

        if (record_counter != 1):

            err_msg  = 'We expect exactly one record and counter shows: ['
            err_msg += str(record_counter) + '] using this SQL: [' + sql + ']' 
            logging.error(err_msg)
            raise Exception(err_msg)

        return(field)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def single_field_update(db_name, table_name, field_name, record_id, new_field_value):

    if os.path.isfile(db_name):

        sql =  'UPDATE ' + table_name + ' SET ' + field_name + '=? WHERE id=?'
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql, (new_field_value, record_id))
        connection.commit()
        connection.close()

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)


#############################################################################
def suite_cleanup_end(db_name, suite_id):

    single_field_update(db_name, 'suite', 'cleanup_end_sse', suite_id, str(int(time())))

##############################################################################
def suite_cleanup_start(db_name, suite_id):

    single_field_update(db_name, 'suite', 'cleanup_start_sse', suite_id, str(int(time())))
    single_field_update(db_name, 'suite', 'phase', suite_id, 'C')

##############################################################################
def suite_end(db_name, suite_id):

    single_field_update(db_name, 'suite', 'suite_end_sse', suite_id, str(int(time())))

##############################################################################
def suite_load_end(db_name, suite_id):

    single_field_update(db_name, 'suite', 'load_end_sse', suite_id, str(int(time())))

##############################################################################
def suite_load_start(db_name, suite_id):

    single_field_update(db_name, 'suite', 'load_start_sse', suite_id, str(int(time())))
    single_field_update(db_name, 'suite', 'phase', suite_id, 'L')

#############################################################################
def suite_main_end(db_name, suite_id):

    single_field_update(db_name, 'suite', 'main_end_sse', suite_id, str(int(time())))

##############################################################################
def suite_main_start(db_name, suite_id):

    single_field_update(db_name, 'suite', 'main_start_sse', suite_id, str(int(time())))
    single_field_update(db_name, 'suite', 'phase', suite_id, 'M')

##############################################################################
def suite_setup_end(db_name, suite_id):

    single_field_update(db_name, 'suite', 'setup_end_sse', suite_id, str(int(time())))

##############################################################################
def suite_setup_start(db_name, suite_id):

    single_field_update(db_name, 'suite', 'setup_start_sse', suite_id, str(int(time())))
    single_field_update(db_name, 'suite', 'phase', suite_id, 'S')

##############################################################################
def suite_start(db_name, suite_id):

    single_field_update(db_name, 'suite', 'suite_start_sse', suite_id, str(int(time())))

#############################################################################
def update_assertion_status(db_name, program_id, program_phase, testware_program_key, status):

    if os.path.isfile(db_name):

        sql =  'SELECT id FROM assertion '
        sql += 'WHERE program_id=' + program_id + ' '
        sql += 'AND program_phase="' + program_phase + '" '
        sql += 'AND status="X" '
        sql += 'AND final_status_sse="0" '
        sql += 'AND testware_program_key="' + testware_program_key + '" '
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)
        #####
        # Ensure that the SQL only returns one record here. If this isn't the case,
        # we'll throw an Exception.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            assertion_id = row[0]

        connection.close()

        if (record_counter != 1):

            err_msg  = "We expect exactly one record and counter shows: ["
            err_msg += str(record_counter) + "]"
            logging.error(err_msg)
            raise Exception(err_msg)

        #####
        # Now, check that only a valid status was passed in.
        #####
        if status == 'P':

            pass

        elif status == 'F':

            pass

        else:

            err_msg = 'There are only two valid values for assertion updates: [P, F]'
            logging.error(err_msg)
            raise Exception(err_msg)

        single_field_update(db_name, 'assertion', 'status', str(assertion_id), status)
        single_field_update(db_name, 'assertion', 'final_status_sse', str(assertion_id), str(int(time())))

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def update_program_status(db_name, program_id, program_phase, specific_status, status):

    #####
    # Check the "specific_status" parameter right away.
    #####
    if specific_status == 'overall_status':

        pass

    elif specific_status == 'ident_status':

        pass

    elif specific_status == 'setup_status':

        pass

    elif specific_status == 'main_status':

        pass

    elif specific_status == 'cleanup_status':

        pass

    else:

        err_msg = 'Only [overall, ident, setup, main, cleanup] specific status types allowed'
        logging.error(err_msg)
        raise Exception(err_msg)

    #####
    # Now, check that only a valid status was passed in.
    #####
    if status == 'P':

        pass

    elif status == 'F':

        pass

    else:

        err_msg = 'There are only two valid values for program status updates: [P, F]'
        logging.error(err_msg)
        raise Exception(err_msg)

    if os.path.isfile(db_name):

        sql =  'SELECT id FROM program '
        sql += 'WHERE id=' + program_id + ' '
        sql += 'AND phase="' + program_phase + '" '
        sql += 'AND ' + specific_status + '="X" '
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)
        #####
        # Ensure that the SQL only returns one record here. If this isn't the case,
        # we'll throw an Exception.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            program_id = row[0]

        connection.close()

        if (record_counter != 1):

            err_msg = 'Expecting 1 record to be returned from: [' + sql + '] and got: [' + str(record_counter) + ']'
            logging.error(err_msg)
            raise Exception(err_msg)

        single_field_update(db_name, 'program', specific_status, str(program_id), status)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
def update_suite_status(db_name, suite_id, suite_phase, specific_status, status):

    #####
    # Check the "specific_status" parameter right away.
    #####
    if specific_status == 'overall_status':

        pass

    elif specific_status == 'setup_status':

        pass

    elif specific_status == 'main_status':

        pass

    elif specific_status == 'cleanup_status':

        pass

    else:

        err_msg = 'Only [overall, setup, main, cleanup] specific status types allowed'
        logging.error(err_msg)
        raise Exception(err_msg)

    #####
    # Now, check that only a valid status was passed in.
    #####
    if status == 'P':

        pass

    elif status == 'F':

        pass

    else:

        err_msg = 'There are only two valid values for suite status updates: [P, F]'
        logging.error(err_msg)
        raise Exception(err_msg)

    if os.path.isfile(db_name):

        sql =  'SELECT id FROM suite '
        sql += 'WHERE id=' + suite_id + ' '
        sql += 'AND phase="' + suite_phase + '" '
        sql += 'AND ' + specific_status + '="X" '
        logging.debug("SQL: [" + sql + "]")
        connection = sqlite.connect(db_name)
        cursor = connection.cursor()
        cursor.execute(sql)
        #####
        # Ensure that the SQL only returns one record here. If this isn't the case,
        # we'll throw an Exception.
        #####
        record_counter = 0 
        for row in cursor:

            record_counter += 1
            program_id = row[0]

        connection.close()

        if (record_counter != 1):

            err_msg = 'Expecting 1 record to be returned from: [' + sql + '] and got: [' + str(record_counter) + ']'
            logging.error(err_msg)
            raise Exception(err_msg)

        single_field_update(db_name, 'suite', specific_status, str(suite_id), status)

    else:

        err_msg  = "We expect [" + db_name + "] to exist and it apparently does not." 
        logging.error(err_msg)
        raise Exception(err_msg)

#############################################################################
if __name__ == "__main__":

    raise Exception('Nothing in main yet')
    logging.debug('example DEBUG msg')
    logging.info('example INFO msg')
    logging.warning('example WARNING msg')
    logging.error('example ERROR msg')
    logging.critical('example CRITICAL msg')

#############################################################################
############################ EOF ############################################
#############################################################################
