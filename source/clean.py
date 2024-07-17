#!/usr/bin/env python
import os
from argparse import ArgumentParser
import sys
import xomlib.xomlibutils as xomlibutils

def main():
    print()
    print("--------------------------------------")
    print("XOM CLEAN DB AND FOLDER module        ")
    print("--------------------------------------")
    print()

    parser = ArgumentParser("clean")
    parser.add_argument("--prefix",type=str, help="name of the prefix")
    parser.add_argument("--db",type=str, help="name of the DB to delete", choices=['data', 'todo', 'tocheck', 'submitted','done','all'], required=True)
    parser.add_argument("--job_folder",type=str, help="path of the job files")
    args = parser.parse_args()
    prefix = args.prefix
    dbtodel = args.db
    job_folder = args.job_folder

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
            
    print('deleting DB with prefix: ', prefix)
    # connect to databases:
    dbs = {}
    for dbsuffix in ['todo','done','submitted','tocheck','data',]:
        dbname = prefix + 'xom' + dbsuffix
        if dbtodel ==dbsuffix or dbtodel == 'all':
            print(f'deleting {dbname}')            
            db = xomlibutils.connect_db(dbname)
            db.delete()

    
    # # here erase the job files in case we remove all the data bases
    if dbtodel == 'all':
        print(f'deleting the job files in folder {job_folder}')
        xomlibutils.empty_directory(job_folder, prefix)
        
 
if __name__ == "__main__":
    main()
