#!/usr/bin/env python
import os
from argparse import ArgumentParser
from socket import timeout
import sys
import configparser

cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../")
import xomlib.constant as constant
import xomlib.dblib as dbl
import analysis as analysis
import xomlib.xomlibutils as xomlibutils
import xomutils as xomutils


def main():
    print()
    print("--------------------------------------")
    print("XOM CLEAN DB AND FOLDER module        ")
    print("--------------------------------------")
    print()

    parser = ArgumentParser("clean")
    parser.add_argument("--config",type=str, help="name of the config file", default='tool_config.json')
    args = parser.parse_args()
    config_file = args.config
    config_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config/")
    tool_config = xomutils.read_json(config_dir + config_file)
    prefix =tool_config['prefix']
    if 'prod' in prefix:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("BEWARE: You are asking to clean the production data bases ")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        user_input = input("Do you want to continue? (yes/no): ")                                                                                                                        
        if user_input.lower() == "yes":
            print("Continuing...")
        else:
            print("Exiting...")
            sys.exit()
    [xomdb,xomdbtodo,xomdbdone,xomdbsubmitted,xomdbtocheck] = xomlibutils.connect_dbs(prefix)

    xomdb.delete()
    xomdbtodo.delete()
    xomdbdone.delete()
    xomdbsubmitted.delete()
    xomdbtocheck.delete()

    
    # here erase the job files
    toberemoved = tool_config['job_folder']
    xomutils.empty_directory(toberemoved, prefix)
    

if __name__ == "__main__":
    main()
