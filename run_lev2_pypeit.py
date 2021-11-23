from copy import copy
from pathlib import Path
import os
from multiprocessing import Pool
from argparse import ArgumentParser
from numpy import source

from pypeit.pypeitsetup import PypeItSetup
from pypeit import msgs, par, pypeit
from pypeit.scripts import run_pypeit
from yaml import parse

instrument_options = {
    "keck_deimos" : "DE.",
    # "keck_mosfire" : "MO.",
    # "keck_nires",
    # "keck_hires",
    # "keck_esi"
}

def get_parsed_args():
    
    parser = ArgumentParser()

    inst_options = ", ".join(instrument_options.keys())
    default_input = os.getcwd()
    default_output = os.path.join(default_input, "redux")
    
    parser.add_argument('inst', help=f'Instrument. Options are: [{inst_options}]')
    parser.add_argument('-i', '--input-dir', dest='input', default=default_input, help='Path to raw files. Defaults to current directory')
    parser.add_argument('-o', '--output-dir', dest='output', default=default_output, help='Directory to put output in. Defaults to ./redux')
    parser.add_argument('-r', '--root', dest='root', help='Base root of the files. E.g. "DE.", "KB.", "kb"')
    parser.add_argument('-n', '--num-proc', dest='num_proc', type=int, help='number of processes to launch')
    parser.add_argument('--setup-only', dest='setup', action='store_true', help="Only create the pypeit files, don't reduce them")
    
    pargs =  parser.parse_args()

    if pargs.root is None:
        pargs.root = instrument_options[pargs.inst]

    return pargs

def generate_pypeit_files(pargs):    

    setup_dir = os.path.join(pargs.input, "setup_files")
    root = os.path.join(pargs.input, pargs.root)

    print(f'Looking for files matching {root}*.fits*')
    print(f'Outputs will be saved om {setup_dir}')

    ps = PypeItSetup.from_file_root(root, pargs.inst, extension=".fits", output_path=setup_dir)
    ps.user_cfg = ['[rdx]', 'ignore_bad_headers = True']

    ps.run(setup_only=True, calibration_check=False, sort_dir=setup_dir, obslog=True)

    ps.fitstbl.write_pypeit(setup_dir, configs='all')

def run_pypeit(pypeit_file, pargs):

    print(f"Processing config from {str(pypeit_file)}")
    logname = os.path.splitext(pypeit_file)[0] + '.log'
    pypeIt = pypeit.PypeIt(pypeit_file, verbosity=1,
                        redux_path=pargs.output,
                        logname=logname)
    pypeIt.reduce_all()
    print(f"Completed {pypeit_file}")

if __name__ == "__main__":
    
    pargs = get_parsed_args()
    
    generate_pypeit_files(pargs)
    
    setup_files = Path(pargs.input) / 'setup_files'
    pypeit_files = list(setup_files.rglob(f'{pargs.inst}_?.pypeit'))
    args = []

    print("Found the following .pypeit files:")
    for f in pypeit_files:
        print(f'    {f}')
        new_pargs = copy(pargs)
        new_pargs.output = os.path.join(pargs.output, os.path.basename(f))
        args.append((f, pargs))
    
    print(f'Args are:')
    

    if not pargs.setup:
        print(f"Launching {pargs.num_proc if pargs.num_proc else os.cpu_count()} processes to reduce {len(pypeit_files)} configurations")
        with Pool(processes=pargs.num_proc) as pool:
            pool.starmap(func=run_pypeit, iterable=args)


        
