import strax
import straxen
import cutax
from argparse import ArgumentParser
import sys
import os

import xomutils as xomutils

def main():
    '''check the data availability according the data type needed and the runid.

    the storage included should be working for the containers 2023.07.2 and 2023.05.2 
    '''

    parser = ArgumentParser()
    parser.add_argument('run_id', type=str)
    parser.add_argument('--available', nargs='+', type=str, default=[])
    args = parser.parse_args()
    
    available_type = args.available
    run_id = args.run_id

    # XOM standard way to load context for data availability consistency
    st = xomutils.load_straxen()

    is_stored = True
    for data_type in available_type:
        is_stored = is_stored & st.is_stored(run_id,data_type)

    if is_stored:
        print("data_is_available")


if __name__ == "__main__":
    main()
    
