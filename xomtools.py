#!/usr/bin/env python
import os
from argparse import ArgumentParser
import sys
cdir =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "./")
import source.xomutils as xomutils
 
 


def main():

    print()
    print("----------------------")
    print("   XOM check tools    ")
    print("----------------------")
    print()
    
    parser = ArgumentParser("xomtools", add_help=False)
    parser.add_argument("prefix",type=str, help="prefix, not used for logs", default='prod_')
    parser.add_argument("--showdbs",action='store_true', help="show the last entries of the database")
    parser.add_argument("--n", '-n',type=int, help="number of entries to show", default=5)
    args = parser.parse_args()
    showdbs = args.showdbs
    prefix = args.prefix
    n = args.n


    if showdbs:
        for db in ['xomtodo', 'xomtocheck', 'xomdata', 'xomsubmitted', 'xomdone']:
            dbfullname = prefix + db
            xomutils.print_db_entries(dbfullname, n)


if __name__ == "__main__":
    main()
