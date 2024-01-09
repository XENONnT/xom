#!/usr/bin/env python
import os
from argparse import ArgumentParser
from socket import timeout
#from utilix.config import Config
from datetime import timezone, datetime, timedelta
import sys
import pprint 
import numpy as np
import time
import configparser
import shlex
import subprocess
from utilix import xent_collection


cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./")

import xomlib.constant as constant
import xomlib.dblib as dbl

import source.xomutils as xomutils
 
 


def main():

    print()
    print("----------------------")
    print("   XOM check tools    ")
    print("----------------------")
    print()

    main_parser = ArgumentParser("xomtools", add_help=False)
    main_parser.add_argument("--showdb",action='store_true', help="show the last entries of the database")
    main_parser.add_argument("--showdbs",action='store_true', help="show the last entries of the database")
    main_parser.add_argument("--showlog",action='store_true', help="show the last lines of the log file of the manager")
    main_parser.add_argument("--n", '-n',type=int, help="number of entries to show", default=5)
    main_args,_ = main_parser.parse_known_args()
    showdb = main_args.showdb
    showdbs = main_args.showdbs
    showlog = main_args.showlog
    n = main_args.n

    if showdb:
        parser = ArgumentParser(parents=[main_parser])

        parser.add_argument("--prefix",type=str, help="",default='prod_', required=showdb) 
        parser.add_argument("--db",type=str, help="db name",choices=constant.list_of_db, required=showdb)
        args = parser.parse_args()
        prefix = args.prefix
        db = args.db
        dbfullname  = prefix + db
        xomutils.print_db_entries(dbfullname, n)

    if showdbs:
        parser = ArgumentParser(parents=[main_parser])
        parser.add_argument("--prefix",type=str, help="", required=showdbs)
        args = parser.parse_args()
        prefix = args.prefix
        for db in constant.list_of_db:
            dbfullname = prefix + db
            xomutils.print_db_entries(dbfullname, n)
    
    if showlog:
        logfile = cdir+ '/logs/xommanager.log'
        stream = os.popen('tail '+ logfile + ' -n ' +str(n))
        output = stream.read()
        print(output)


#    if show



if __name__ == "__main__":
    main()
