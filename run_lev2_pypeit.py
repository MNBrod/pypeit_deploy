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
    parser.add_argument('inst', help=f'Options are: [{inst_options}]')
    parser.add_argument('-i', '--input-dir', dest='input')
    parser.add_argument('-o', '--output-dir', dest='output')
    parser.add_argument('-r', '--root', dest='root')
    parser.add_argument('-n', '--num-proc', dest='num_proc', help='number of processes to launch')
    parser.add_argument('--setup-only', dest='setup', default=False)
    
    pargs =  parser.parse_args()

    if pargs.root is None:
        pargs.root = instrument_options[pargs.inst]

    if pargs.input is None:
        pargs.input = os.getcwd()
        
    if pargs.output is None:
        pargs.output = os.path.join(pargs.input, "redux")

def generate_pypeit_files(pargs):    

    setup_dir = os.path.join(pargs.input, "setup_files")
    ps = PypeItSetup.from_file_root(pargs.root, pargs.inst, extension=".fits", output_path=setup_dir)
    ps.user_cfg = ['[rdx]', 'ignore_bad_headers = True']

    ps.run(setup_only=False, calibration_check=False, sort_dir=setup_dir, obslog=True)

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
    
    generate_pypeit_files(pargs.inst,
                            input_dir = pargs.input,
                            output_dir = pargs.output,
                            root = pargs.root)
    
    setup_files = Path(pargs.input) / 'setup_files'
    pypeit_files = setup_files.rglob('*.pypeit')
    args = []

    print("Found the following .pypeit files:")
    for f in pypeit_files:
        print(f'    {f}')
        args.append((f, pargs))

    if not pargs.setup:
        print(f"Launching {pargs.num_proc} processes to reduce {len(setup_files)} configurations")
        with Pool(processes=pargs.num_proc) as pool:
            pool.starmap(func=run_pypeit, iterable=args)


        
