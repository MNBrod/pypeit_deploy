from copy import copy
from datetime import datetime
from pathlib import Path
import os
import sys
import requests
from multiprocessing import Pool
from argparse import ArgumentParser
from configparser import ConfigParser
import subprocess

from pypeit.pypeitsetup import PypeItSetup


###
##### PypeIt Stuff
###


def generate_pypeit_files(pargs, cfg):   
    """Creates the a .pypeit file for every configuration identified in the input files

    Parameters
    ----------
    pargs : Parsed command line arguments
        Should be the output from get_parsed_args()
    """     

    setup_dir = os.path.join(pargs.input, "pypeit_files")
    root = os.path.join(pargs.input, pargs.root)

    print(f'Looking for files matching {root}*.fits*')
    print(f'Outputs will be saved om {setup_dir}')

    # Create the setup object
    ps = PypeItSetup.from_file_root(root, pargs.inst, extension=".fits", output_path=setup_dir)
    ps.user_cfg = ['[rdx]', 'ignore_bad_headers = True']

    # Run the setup
    ps.run(setup_only=True, calibration_check=False, sort_dir=setup_dir, obslog=True)

    # Save the setup to .pypeit files
    ps.fitstbl.write_pypeit(setup_dir, configs='all')


def run_pypeit_helper(pypeit_file, pargs, cfg):
    """Runs a PypeIt reduction off of a specific .pypeit file, using the io
    parameters in pargs.

    The reduction is launched in a subprocess using the subprocess library, with
    stdout and stderr directed to a single log file. 

    Parameters
    ----------
    pypeit_file : str or pathlike
        .pypeit file to reduce
    pargs : Parsed command line arguments
        Should be from get_parsed_args()
    """

    print(f"Processing config from {str(pypeit_file)}")

    # Open file to dump logs into
    logname = os.path.splitext(pypeit_file)[0] + '.log'
    logpath = os.path.join(pargs.output, logname)
    f = open(logpath, 'w+')
    
    # Get full output path
    outputs = os.path.join(pargs.output, os.path.splitext(pypeit_file)[0])
    
    # Run the reduction in a subprocess
    args = ['run_pypeit']
    args += [pypeit_file]
    args += ['-r', str(outputs)]
    args += ['-o']
    proc = subprocess.run(args, stdout=f, stderr=f)

    if proc.returncode != 0:
        print(f"Error encountered while reducing {pypeit_file}")
        print(f"Log can be found at {logpath}")
    else:
        print(f"Reduced {pypeit_file}")
        alert_RTI(outputs, pargs, cfg)
    f.close()


###
##### RTI Stuff
###


def alert_RTI(directory, pargs, cfg):

    def get_url(url, data):
        try:
            res = requests.get(url,
                               params = data, 
                               auth = (cfg.user, cfg.pw))
            print(f"Sending {res.request.url}")
        except requests.exceptions.RequestException as e:
            print(f"Error caught while posting to {url}:")
            print(e)
            return None
        return res
    
    data_directory = pargs.output
    
    print(f"Alerting RTI that {directory} is ready for ingestion")

    url = cfg['RTI'].url

    data = {
        'instrument': pargs.inst,
        'koaid': "KOAID_HERE", # This won't be KOAID anymore, will it?
        'ingesttype': cfg['RTI']['rti_ingesttype'],
        'datadir': str(data_directory),
        'start': str(cfg.start_time),
        'reingest': cfg['RTI']['rti_reingest'],
        'testonly': cfg['RTI']['rti_testonly'],
        'dev': cfg['RTI']['rti_dev']
    }
    
   
    res = get_url(url, data)
    

###
##### Script Stuff
###

def get_config(cfg_file):

    cfg = ConfigParser(cfg_file)
    
    inst_options = cfg['INSTRUMENTS']['keck_inst_names'].split(' ')
    inst_pypeit = cfg['INSTRUMENTS']['pypeit_inst_names'].split(' ')
    inst_roots = cfg['INSTRUMENTS']['roots'].split(' ')
    cfg.inst_opts = {
        inst_options[i] : {
            'pypeit_name' : inst_pypeit[i],
            'root' : inst_roots[i]
        }
    for i in range(len(inst_options))}

    cfg.start_time = datetime.utcnow()

    return cfg

def get_parsed_args():
    """Returns the parsed command line arguments

    Returns
    -------
    argparse NameSpace
        contains all of the parsed arguments
    """
    
    parser = ArgumentParser()
    
    # If nothing else is supplied, script will look for data in cwd
    default_input = os.getcwd()
    default_output = os.path.join(default_input, "redux")

    parser.add_argument('inst', help=f'Instrument choice. To see availble instruments, use --instrument-options')

    parser.add_argument('-i', '--input-dir', dest='input', default=default_input, help='Path to raw files. Defaults to current directory')
    parser.add_argument('-o', '--output-dir', dest='output', default=default_output, help='Directory to put output in. Defaults to ./redux')
    parser.add_argument('-r', '--root', dest='root', help='Base root of the files. E.g. "DE.", "KB.", "kb". If none, will attempt to find a suitable root from the config.')
    parser.add_argument('-n', '--num-proc', dest='num_proc', type=int, help='number of processes to launch')
    parser.add_argument('-c', '--config', dest='cfg_file', default='./pypeit_lev2.ini', help='Config file to use')
    parser.add_argument('--setup-only', dest='setup', action='store_true', help="Only create the pypeit files, don't reduce them")
    parser.add_argument('--instrument-options', dest='opts', action='store_true', help='prints the instruments this script can reduce')
    
    pargs =  parser.parse_args()

    return pargs


def main():

    
    # Parse the arguments
    pargs = get_parsed_args()
    
    # Get configuration
    print(pargs.cfg_file)
    cfg = ConfigParser.read(pargs.cfg_file)

    if pargs.opts:
        inst_options = "', ".join(cfg.inst_opts.keys())
        print(f"Options are: '{inst_options}'")
        sys.exit(0)

    # If no root is specified, get it from the instruments list
    if pargs.root is None:
        pargs.root = cfg.inst_opts[pargs.inst]['root']


    # Create all the pypeit files
    generate_pypeit_files(pargs)
    
    setup_files = Path(pargs.input) / 'pypeit_files'
    # Select only the pypeit files that are associated with an instrument configuration
    pypeit_files = list(setup_files.rglob(f'{pargs.inst}_?.pypeit'))
    args = []

    # Create the arguments for the pool mapping function
    print("Found the following .pypeit files:")
    for f in pypeit_files:
        print(f'    {f}')
        new_pargs = copy(pargs)
        new_pargs.output = os.path.join(pargs.output, "redux")
        args.append((f, pargs))
        outputs = os.path.join(pargs.output, os.path.splitext(f)[0])
        print(f"Output file directory for {f}: {outputs}")
    

    if not pargs.setup:
        num = pargs.num_proc if pargs.num_proc else os.cpu_count() - 1
        print(f"Launching {num} processes to reduce {len(pypeit_files)} configurations")

        with Pool(processes=num) as pool:
            pool.starmap(func=run_pypeit_helper, iterable=args)


if __name__ == '__main__':
    main()