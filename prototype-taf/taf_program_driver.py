#!/usr/bin/python
#############################################################################

#####
# TAF modules.
#####
import taf_rdbms
import taf_utility

import logging

from os import environ
#############################################################################
################ Class - taf_program_driver ###############################
#############################################################################
class taf_program_driver:

    #########################################################################
    def __init__(self, taf_sqlite_db_name, taf_program_id):

        self.sqlite_db_name = taf_sqlite_db_name
        self.program_id = taf_program_id

        self.internal_debug_output = False
        
        self.program_name = taf_rdbms.lookup_program_name_by_program_id(self.sqlite_db_name, self.program_id)
        self.program_args = taf_rdbms.lookup_program_arguments_by_program_id(self.sqlite_db_name, self.program_id)
        self.program_random_gen = taf_rdbms.lookup_program_randomly_generated_by_program_id(self.sqlite_db_name, self.program_id)
        self.program_sequential = taf_rdbms.lookup_program_sequential_by_program_id(self.sqlite_db_name, self.program_id)
        self.suite_id = taf_rdbms.lookup_program_suite_id_by_program_id(self.sqlite_db_name, self.program_id)
        self.suite_sof = taf_rdbms.lookup_suite_stop_on_failure_by_suite_id(self.sqlite_db_name, self.suite_id)
        self.overall_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'overall_status')

        environ['TAF_PROGRAM_ID'] = self.program_id
        environ['TAF_PROGRAM_RANDOMLY_GENERATED'] = self.program_random_gen
        environ['TAF_STOP_ON_FAILURE'] = self.suite_sof

    #########################################################################
    def debug_output(self, boolean_value):

        self.internal_debug_output = boolean_value

    #########################################################################
    def print_environment_variables(self):

        msg_str  = "\n------------------------------------------------------------------------------\n"
        msg_str += 'TAF_FILE                       : [' + str(environ.get('TAF_FILE')) + ']\n'
        msg_str += 'TAF_PROGRAM_ID                 : [' + str(environ.get('TAF_PROGRAM_ID')) + ']\n'
        msg_str += 'TAF_PROGRAM_PHASE              : [' + str(environ.get('TAF_PROGRAM_PHASE')) + ']\n'
        msg_str += 'TAF_PROGRAM_PHASE_TIMEOUT      : [' + str(environ.get('TAF_PROGRAM_PHASE_TIMEOUT')) + ']\n'
        msg_str += 'TAF_PROGRAM_RANDOMLY_GENERATED : [' + str(environ.get('TAF_PROGRAM_RANDOMLY_GENERATED')) + ']\n'
        msg_str += 'TAF_STOP_ON_FAILURE            : [' + str(environ.get('TAF_STOP_ON_FAILURE')) + ']\n'
        msg_str += "------------------------------------------------------------------------------\n"
        logging.debug(msg_str)

    #########################################################################
    def print_instance_variables(self):

        msg_str  = "\n------------------------------------------------------------------------------\n"
        msg_str += 'Program Arguments                : [' + self.program_args + ']\n'
        msg_str += 'Program Id                       : [' + self.program_id + ']\n'
        msg_str += 'Program Name                     : [' + self.program_name + ']\n'
        msg_str += 'Program Randomly Generated       : [' + self.program_random_gen + ']\n'
        msg_str += 'Program Sequential               : [' + self.program_sequential + ']\n'
        msg_str += 'Suite Id                         : [' + str(self.suite_id) + ']\n'
        msg_str += 'Suite SOF Flag                   : [' + self.suite_sof + ']\n'
        msg_str += 'SQLite DB Name                   : [' + self.sqlite_db_name + ']\n'
        msg_str += "------------------------------------------------------------------------------\n"
        logging.debug(msg_str)

    #########################################################################
    def run(self):

        #########################################################################
        ########### Identify Phase ##############################################
        #########################################################################
        taf_rdbms.program_ident_start(self.sqlite_db_name, self.program_id)

        program_phase = taf_rdbms.lookup_program_phase_by_program_id(self.sqlite_db_name, self.program_id)
        environ['TAF_PROGRAM_PHASE'] = program_phase

        program_phase_timeout = taf_rdbms.lookup_program_ident_timeout_by_program_id(self.sqlite_db_name, self.program_id)
        environ['TAF_PROGRAM_PHASE_TIMEOUT'] = program_phase_timeout

        taf_file = 'taf-files\\' + taf_utility.get_random_string() + '-' + self.program_id + '-' + program_phase + '.txt'
        environ['TAF_FILE'] = taf_file

        self.print_environment_variables()
        self.print_instance_variables()

        #####
        # Invoke Test Program With Timeout
        #####
        my_cmd = self.program_name + ' ' + self.program_args
        return_info = taf_utility.run_cmd_with_timeout(my_cmd, int(program_phase_timeout))
        return_code = return_info[0]
        timeout_occurred = return_info[1]

        msg_str  = 'Phase Timeout: [' + program_phase_timeout + '] ' 
        msg_str += 'Return Code: [' + str(return_code) + '] '
        msg_str += 'Timeout Occurred: [' + str(timeout_occurred) + ']'
        logging.debug(msg_str)

        #####
        # For the Ident phase, we are going to call the process_taf_program_output_file() method
        # in a try/except/else structure. For now, we will assume that this phase is successful if 
        # an exception is not thrown.
        #####
        try:

            taf_rdbms.process_taf_program_output_file(self.sqlite_db_name, taf_file)

        except:

            #####
            # An unspecified Exception was thrown
            # FAIL the IDENT status.
            #####
            taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'ident_status', 'F')

        else:

            #####
            # No Exception was thrown
            # Dig deeper and ensure that both IDENTITY related fields are set to something other than 
            # their initial value (e.g. 'X') before declaring this phase to be a PASS. 
            #####
            program_ident_name = taf_rdbms.single_field_select_by_id(self.sqlite_db_name, 'program', 'ident_name', self.program_id)
            program_ident_version = taf_rdbms.single_field_select_by_id(self.sqlite_db_name, 'program', 'ident_version', self.program_id)

            if ((program_ident_name != 'X') and (program_ident_version != 'X')) :

                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'ident_status', 'P')

            else:

                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'ident_status', 'F')

        taf_rdbms.program_ident_end(self.sqlite_db_name, self.program_id)

        #########################################################################
        ########### Setup Phase #################################################
        #########################################################################
        ident_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'ident_status')

        #####
        # We will only execute the SETUP/MAIN/CLEANUP phases if (and only if) the IDENT phase is a PASS.
        # If a Test Program can't even identify itself, why bother further with it?
        #####
        if ident_status == 'P' :

            taf_rdbms.program_setup_start(self.sqlite_db_name, self.program_id)

            program_phase = taf_rdbms.lookup_program_phase_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE'] = program_phase

            program_phase_timeout = taf_rdbms.lookup_program_setup_timeout_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE_TIMEOUT'] = program_phase_timeout

            taf_file = 'taf-files\\' + taf_utility.get_random_string() + '-' + self.program_id + '-' + program_phase + '.txt'
            environ['TAF_FILE'] = taf_file

            self.print_environment_variables()
            self.print_instance_variables()

            #####
            # Invoke Test Program With Timeout
            #####
            my_cmd = self.program_name + ' ' + self.program_args
            return_info = taf_utility.run_cmd_with_timeout(my_cmd, int(program_phase_timeout))
            return_code = return_info[0]
            timeout_occurred = return_info[1]

            msg_str  = 'Phase Timeout: [' + program_phase_timeout + '] ' 
            msg_str += 'Return Code: [' + str(return_code) + '] '
            msg_str += 'Timeout Occurred: [' + str(timeout_occurred) + ']'
            logging.debug(msg_str)

            #####
            # Since the process_taf_program_output_file() can throw exceptions, use a try/except/else
            # structure here.
            #####
            try:

                taf_rdbms.process_taf_program_output_file(self.sqlite_db_name, taf_file)

            except:

                # An unspecified Exception was thrown
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'setup_status', 'F')

            else:

                # No Exception was thrown
                # Dig deeper and delve into the contents of the assertion table to determine the true status for this phase. 
                program_phase_status = taf_rdbms.determine_program_phase_status(self.sqlite_db_name, self.program_id, program_phase)
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'setup_status', program_phase_status)

            taf_rdbms.program_setup_end(self.sqlite_db_name, self.program_id)

        #########################################################################
        ########### Main Phase ##################################################
        #########################################################################
        setup_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'setup_status')

        #####
        # At the Test Program level, we will only execute the MAIN phase if (and only if) the SETUP phase is a PASS
        # If this is not the case, the MAIN phase will be bypassed.
        #####
        if ident_status == 'P' and setup_status == 'P' :

            taf_rdbms.program_main_start(self.sqlite_db_name, self.program_id)

            program_phase = taf_rdbms.lookup_program_phase_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE'] = program_phase

            program_phase_timeout = taf_rdbms.lookup_program_main_timeout_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE_TIMEOUT'] = program_phase_timeout

            taf_file = 'taf-files\\' + taf_utility.get_random_string() + '-' + self.program_id + '-' + program_phase + '.txt'
            environ['TAF_FILE'] = taf_file

            self.print_environment_variables()
            self.print_instance_variables()

            #####
            # Invoke Test Program With Timeout
            #####
            my_cmd = self.program_name + ' ' + self.program_args
            return_info = taf_utility.run_cmd_with_timeout(my_cmd, int(program_phase_timeout))
            return_code = return_info[0]
            timeout_occurred = return_info[1]

            msg_str  = 'Phase Timeout: [' + program_phase_timeout + '] ' 
            msg_str += 'Return Code: [' + str(return_code) + '] '
            msg_str += 'Timeout Occurred: [' + str(timeout_occurred) + ']'
            logging.debug(msg_str)

            #####
            # Since the process_taf_program_output_file() can throw exceptions, use a try/except/else
            # structure here.
            #####
            try:

                taf_rdbms.process_taf_program_output_file(self.sqlite_db_name, taf_file)

            except:

                # An unspecified Exception was thrown
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'main_status', 'F')

            else:

                # No Exception was thrown
                # Dig deeper and delve into the contents of the assertion table to determine the true status for this phase. 
                program_phase_status = taf_rdbms.determine_program_phase_status(self.sqlite_db_name, self.program_id, program_phase)
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'main_status', program_phase_status)

            taf_rdbms.program_main_end(self.sqlite_db_name, self.program_id)

        #########################################################################
        ########### Cleanup Phase ###############################################
        #########################################################################
        main_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'main_status')

        #####
        # Kind of hairy logic below. 
        # We will bypass the CLEANUP phase if the IDENT phase did not PASS.
        # Additionally, we will also bypass the CLEANUP phase if Stop On Failure (SOF) is TRUE AND we have a failure
        # perceived from either the SETUP or MAIN phases.
        #####
        if ( (ident_status == 'P') and not ( (self.suite_sof == '1') and (setup_status == 'F' or main_status == 'F') ) ) :

            taf_rdbms.program_cleanup_start(self.sqlite_db_name, self.program_id)

            program_phase = taf_rdbms.lookup_program_phase_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE'] = program_phase

            program_phase_timeout = taf_rdbms.lookup_program_cleanup_timeout_by_program_id(self.sqlite_db_name, self.program_id)
            environ['TAF_PROGRAM_PHASE_TIMEOUT'] = program_phase_timeout

            taf_file = 'taf-files\\' + taf_utility.get_random_string() + '-' + self.program_id + '-' + program_phase + '.txt'
            environ['TAF_FILE'] = taf_file

            self.print_environment_variables()
            self.print_instance_variables()

            #####
            # Invoke Test Program With Timeout
            #####
            my_cmd = self.program_name + ' ' + self.program_args
            return_info = taf_utility.run_cmd_with_timeout(my_cmd, int(program_phase_timeout))
            return_code = return_info[0]
            timeout_occurred = return_info[1]

            msg_str  = 'Phase Timeout: [' + program_phase_timeout + '] ' 
            msg_str += 'Return Code: [' + str(return_code) + '] '
            msg_str += 'Timeout Occurred: [' + str(timeout_occurred) + ']'
            logging.debug(msg_str)

            #####
            # Since the process_taf_program_output_file() can throw exceptions, use a try/except/else
            # structure here.
            #####
            try:

                taf_rdbms.process_taf_program_output_file(self.sqlite_db_name, taf_file)

            except:

                # An unspecified Exception was thrown
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'cleanup_status', 'F')

            else:

                # No Exception was thrown
                # Dig deeper and delve into the contents of the assertion table to determine the true status for this phase. 
                program_phase_status = taf_rdbms.determine_program_phase_status(self.sqlite_db_name, self.program_id, program_phase)
                taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'cleanup_status', program_phase_status)

            taf_rdbms.program_cleanup_end(self.sqlite_db_name, self.program_id)
        
        #################################################################################################################
        ########### Final, overall status determination for this Test Program ###########################################
        #################################################################################################################
        cleanup_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'cleanup_status')

        if (ident_status == 'P' and setup_status == 'P' and main_status == 'P' and cleanup_status == 'P') :

            taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'overall_status', 'P')

        else:

            taf_rdbms.update_program_status(self.sqlite_db_name, self.program_id, program_phase, 'overall_status', 'F')


        self.overall_status = taf_rdbms.lookup_program_status(self.sqlite_db_name, self.program_id, 'overall_status')

        msg_str = 'Overall Status For Program: [' + self.program_id + '] is: [' + self.overall_status + ']'
        logging.info(msg_str)

#############################################################################
## main #####################################################################
#############################################################################
if __name__ == '__main__':

    raise Exception('Nothing in main yet')

    logging.debug('example DEBUG msg')
    logging.info('example INFO msg')
    logging.warning('example WARNING msg')
    logging.error('example ERROR msg')
    logging.critical('example CRITICAL msg')

#############################################################################
####################### EOF #################################################
#############################################################################
