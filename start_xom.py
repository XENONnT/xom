#!/usr/bin/env python3

import os, shlex, subprocess, configparser, argparse, socket, time

# parse command line arguments (config file)
parser = argparse.ArgumentParser(description='Start XOM.')
parser.add_argument('--prefix', type=str, help='prefix of the measruement in influxdb', default='test_')
parser.add_argument('--clean', action='store_true',help=' clean the called databases')
parser.add_argument('--loglevel',type=str, help="Shows informations and statistics about the database", default='INFO')
args = parser.parse_args()


# main xom apps
apps = {'compare': ' source/proc_compare.py',
        'runner': ' source/proc_runner.py'
    }


# find if there are any screens matching these names
user = os.getenv('USER')
p = subprocess.run(shlex.split(f'ls /var/run/screen/S-{user}'),
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   universal_newlines=True)
screen_list = [ n.partition('.')[2] for n in p.stdout.split() ]

    
if args.clean:
    if args.prefix == 'prod_':
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("BEWARE: You are asking to clean the production data bases ")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        user_input = input("Do you want to continue? (yes/no): ")
        if user_input.lower() == "yes":
            print("Continuing...")
        else:
            print("Exiting...")
            sys.exit()
    p = subprocess.Popen(shlex.split(f'python source/clean.py --prefix {args.prefix}'))
    p.wait()

# start apps
print('>>> Attempting to start XOM apps...')
for app in apps:
    screen_name = args.prefix + app
    if screen_name not in screen_list:        
        rawargs = f'screen -dmS {screen_name} python {apps[app]} --prefix {args.prefix} --loglevel {args.loglevel} '
        cmdargs = shlex.split(rawargs)
        print(f'Executing: {rawargs}')
        p = subprocess.Popen(cmdargs)
    else:
        print(f'Ignored {app} (already running)')
