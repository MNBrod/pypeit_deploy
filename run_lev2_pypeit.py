from copy import copy
from pathlib import Path
import os
import requests
from multiprocessing import Pool
from argparse import ArgumentParser
import subprocess

from pypeit.pypeitsetup import PypeItSetup

# List of valid instrument options and the file prefix associated
instrument_options = {
    "keck_deimos" : "DE.",
    "keck_mosfire" : "MF.",
    # "keck_nires" : "",
    # "keck_hires" : "",
    # "keck_esi" : ""
}

def get_parsed_args():
    """Returns the parsed command line arguments

    Returns
    -------
    argparse NameSpace
        contains all of the parsed arguments
    """
    
    parser = ArgumentParser()

    inst_options = ", ".join(instrument_options.keys())
    
    # If nothing else is supplied, script will look for data in cwd
    default_input = os.getcwd()
    default_output = os.path.join(default_input, "redux")
    
    parser.add_argument('inst', help=f'Instrument. Options are: [{inst_options}]')
    parser.add_argument('-i', '--input-dir', dest='input', default=default_input, help='Path to raw files. Defaults to current directory')
    parser.add_argument('-o', '--output-dir', dest='output', default=default_output, help='Directory to put output in. Defaults to ./redux')
    parser.add_argument('-r', '--root', dest='root', help='Base root of the files. E.g. "DE.", "KB.", "kb"')
    parser.add_argument('-n', '--num-proc', dest='num_proc', type=int, help='number of processes to launch')
    parser.add_argument('--setup-only', dest='setup', action='store_true', help="Only create the pypeit files, don't reduce them")
    
    pargs =  parser.parse_args()

    # If no root is specified, get it from the instruments list
    if pargs.root is None:
        pargs.root = instrument_options[pargs.inst]

    return pargs


def generate_pypeit_files(pargs):   
    """Creates the a .pypeit file for every configuration identified in the input files

    Parameters
    ----------
    pargs : Parsed command line arguments
        Should be the output from get_parsed_args()
    """     

    setup_dir = os.path.join(pargs.input, "setup_files")
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


def run_pypeit(pypeit_file, pargs):
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

    if proc.returncode is not 0:
        print(f"Error encountered while reducing {pypeit_file}")
        print(f"Log can be found at {logpath}")
    else:
        print(f"Reduced {pypeit_file}")
        # Send the http notice here
    f.close()

def alert_RTI(file, url, config, instrument):

    def get_url(url, data):
        try:
            res = requests.get(url,
                               params = data, 
                               auth = (config.user,
                                       config.pw)
                                )
            print(f"Sending {res.request.url}")
        except requests.exceptions.RequestException as e:
            print(f"Error caught while posting to {url}:")
            print(e)
            return None
        return res
    
    data_directory = ""
    
    print(f"Alerting RTI that {file} is ready for ingestion")

    url = url
    data = {
        'instrument': instrument,
        'koaid': "KOAID_HERE",
        'ingesttype': config.rti.rti_ingesttype,
        'datadir': str(data_directory),
        'start': str(config.action.args.ingest_time),
        'reingest': config.rti.rti_reingest,
        'testonly': config.rti.rti_testonly,
        'dev': config.rti.rti_dev
    }
    
   
    res = get_url(url, data)
    


if __name__ == "__main__":
    
    # Parse the arguments
    pargs = get_parsed_args()
    
    # Create all the pypeit files
    generate_pypeit_files(pargs)
    
    setup_files = Path(pargs.input) / 'setup_files'
    # Select only the pypeit files that are associated with an instrument configuration
    pypeit_files = list(setup_files.rglob(f'{pargs.inst}_?.pypeit'))
    args = []

    # Create the arguments for the pool mapping function
    print("Found the following .pypeit files:")
    for f in pypeit_files:
        print(f'    {f}')
        new_pargs = copy(pargs)
        new_pargs.output = os.path.join(pargs.output, os.path.basename(f))
        args.append((f, pargs))
    

    if not pargs.setup:
        num = pargs.num_proc if pargs.num_proc else os.cpu_count() - 1
        print(f"Launching {num} processes to reduce {len(pypeit_files)} configurations")

        with Pool(processes=num) as pool:
            pool.starmap(func=run_pypeit, iterable=args)


        
