#!/usr/bin/python
#############################################################################
# According to this:
# http://stackoverflow.com/questions/12735392/python-class-static-methods
# the "Pythonic" way to handle a bunch of utilities is to not put them into
# a "class", but to just put them into a file to import.
#############################################################################
import shlex
import subprocess

from datetime import datetime
from getpass import getuser
from platform import node
from random import randrange
from sys import exit, maxint
from threading import Timer

#############################################################################
################ taf_utilities ############################################
#############################################################################
def get_current_user():

    #####
    # Acquire the current user if possible. If we can't acquire this data 
    # element, we will use the literal string 'unavailable'.
    #####
    current_user = getuser()

    if not current_user:

        current_user = 'unavailable'

    return(current_user)

#############################################################################
def get_machine_name():

    #####
    # Acquire the machine name if possible. If we can't acquire this data 
    # element, we will use the literal string 'unavailable'.
    #####
    machine_name = node()

    if not machine_name:

        machine_name = 'unavailable'

    return(machine_name)

#############################################################################
def get_random_string():

    #####
    # Acquire a datetime stamp down to the micro second level.
    #####
    date_time_stamp = datetime.now().strftime("%Y%m%d%H%M%S-%f")

    #####
    # Generate a pseudo random number from 1 to "maxint" on this machine.
    #####
    random_number = str(randrange(1, maxint))
    
    #####
    # Concatenate them together and return. 
    #####
    random_string = date_time_stamp + '-' + random_number

    return random_string

#############################################################################
# Example code in public domain:
# http://stackoverflow.com/questions/1191374/subprocess-with-timeout
#############################################################################
def kill_proc(proc, timeout):

    timeout["value"] = True
    proc.kill()

#############################################################################
# Example code in public domain:
# http://stackoverflow.com/questions/1191374/subprocess-with-timeout
#############################################################################
def run_cmd_with_timeout(cmd, timeout_sec):

    proc = subprocess.Popen(shlex.split(cmd))
    timeout = {"value": False}
    timer = Timer(timeout_sec, kill_proc, [proc, timeout])
    timer.start()
    stdout, stderr = proc.communicate()
    timer.cancel()
    return proc.returncode, timeout["value"]

#############################################################################
def strip_trailing_newlines(input_str):
   
    #####
    # Strip off the trailing newlines. Works on either Mac, Windows, or Unix.
    # According to this:
    # http://stackoverflow.com/questions/275018/how-can-i-remove-chomp-a-newline-in-python
    #####
    return(input_str.rstrip('\r\n'))

#############################################################################
########################### main ############################################
#############################################################################
if __name__ == '__main__':

    raise Exception('Nothing in main yet')

#############################################################################
####################### EOF #################################################
#############################################################################
