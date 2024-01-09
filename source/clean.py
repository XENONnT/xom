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
    parser.add_argument("--prefix",type=str, help="define the database name you want to write in, for instance test", default='')
    args = parser.parse_args()
    prefix = args.prefix
    [xomdb,xomdbtodo,xomdbdone,xomdbsubmitted,xomdbtocheck] = xomlibutils.connect_dbs(prefix)
    
    xomdb.delete()
    xomdbtodo.delete()
    xomdbdone.delete()
    xomdbsubmitted.delete()
    xomdbtocheck.delete()

    config_dir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./../config/")
    tool_config = xomutils.read_json(config_dir + 'tool_config.json')
    
    # here erase the job files
    toberemoved = tool_config['job_folder']
    xomutils.empty_directory(toberemoved, prefix)
    

if __name__ == "__main__":
    main()
