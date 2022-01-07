#pylint: disable=line-too-long, trailing-whitespace, invalid-name, missing-module-docstring

import argparse
import configparser
import json
import logging
import sys
from pathlib import Path
from typing import List

#################### Some setup ####################

THIS_FILE = Path( __file__ ).resolve()
CFG_FILE  = THIS_FILE.parent / 'pypeline.config.ini'
LOGLEVEL = logging.INFO
LOG_FORMAT = "[%(levelname)s:%(funcName)s] %(message)s"
logging.basicConfig(level=LOGLEVEL, format=LOG_FORMAT)
LOG = logging.getLogger( __file__ )

CFG = {}

#################### Helper functions ####################

def file_collector( root: Path ) -> Path:
    ''' Easy to use tool to collect files matching a pattern (recursive or not), using pathlib.glob.
    Collect files matching given pattern(s) '''

    for item in root.glob( '**/*.*'):
        if not item.is_file():
            continue
        
        yield item


def find_available_path( root: Path, base_name: str, file: bool = True ) -> Path:
    ''' Returns a path to a file/directory that DOESN'T already exist.
    The file/dir the user wishes to make a path for is referred as X.
    `root`: where X must be created. Can be a list of path parts
    `base_name`: the base name for X. May be completed with '(index)' if name already exists.
    
    `file`: True if X is a file, False if it is a directory
    '''
    # Helper function: makes suffixes for already existing files/directories
    def suffixes():
        yield ''
        idx=0
        while True:
            idx+=1
            yield f" ({idx})"
    
    # Iterate over candidate paths until an unused one is found
    if file:
        # name formatting has to keep the extension at the end of the name !
        ext_idx = base_name.rfind('.')
        f_name, f_ext = (base_name[:ext_idx], base_name[ext_idx:]) if ext_idx!=-1 else (base_name, '')
        for suffix in suffixes():
            _object = root / ( f_name + suffix + f_ext )
            if not _object.is_file():
                return _object
    else:
        for suffix in suffixes():
            _object = root / ( base_name + suffix )
            if not _object.is_dir():
                return _object


def write_default_config() -> None:
    ''' Writes a structurally valid config file for the
    user to edit.
    '''
    cfg_dict = {
        'Settings': {
            'sources_help': "List paths for all directories to assume the `source` role",
            'sources': [ "/home/example_user_1/" ],
            'destination_help': "Path of the directory to assume the `destination` role",
            'destination': "/home/example_user_2/",
            'directory_structure_help': "A complete description of the directory structure of the pipeline (one root; nested structure)",
            'directory_structure': { 'example': { 'videos': { 'movies': {}, 'series': {} }, 'photos': {} } }
        }
    }
    cfg = configparser.ConfigParser()
    for section, entries in cfg_dict.items():
        cfg.add_section(section)
        for entry_name, entry_value in entries.items():
            val = entry_value if isinstance(entry_value, str) else json.dumps(entry_value)
            cfg.set( section, entry_name, val )

    with CFG_FILE.open( 'w', encoding='utf8' ) as f:
        cfg.write(f)


def cli_args() -> argparse.Namespace:
    ''' Parses CLI arguments using argparse
    '''
    parser = argparse.ArgumentParser(
        prog='Pypeline (dev)',
        description=__doc__
    )
    parser.add_argument(
        '--remove_directories',
        action='store_true',
        help="Remove Pypeline directories (must be empty!)"
    )
    return parser.parse_args()


def print_cfg() -> None:
    ''' For debug purposes
    '''
    msg = "Config:\n" + '\n'.join( f"> '{k}': {v} ({type(v)})" for k,v in CFG.items() )
    LOG.info( msg )


def expand_directory_structure( root: Path ) -> List[Path]:
    ''' Returns a list of paths for the complete directory sructure
    expanded from ``root``
    '''

    def expand_dirs_rec( _root: Path, _dir_structure: dict ) -> List[Path]:
        paths = [ _root ]
        for subdir, subdir_structure in _dir_structure.items():
            paths += expand_dirs_rec( _root / subdir, subdir_structure )

        return paths

    assert isinstance(root, Path)
    res = expand_dirs_rec( root, CFG['directory_structure'] )
    res.remove( root )
    return res


def read_config() -> None:
    ''' Reads config from 'pypeline.config.ini'
    Writes a default config file if it doesn't exist.
    '''

    # case: no CFG file
    if not CFG_FILE.is_file():
        print(f"Could not find file '{CFG_FILE}' ! Writing a default config file ..")
        write_default_config()
        print("Please fill the config file and re-launch pypeline.")
        sys.exit(0)

    config = configparser.ConfigParser()		
    config.read( CFG_FILE, encoding='utf8' )

    assert config.has_section('Settings')
    CFG['destination'] = Path(config.get('Settings', 'destination'))
    CFG['sources'] = [ Path(p) for p in json.loads(config.get('Settings', 'sources', raw=True)) ]
    CFG['directory_structure'] = json.loads(config.get('Settings', 'directory_structure', raw=True))


def ensure_role_paths() -> None:
    ''' Ensures that role (source/destination) directories actually exist, 
    that there is no incoherence (destination dir one of source dir) and
    creates pypeline directory structure.
    Raise AssertionError on directory not found or incoherence detected
    '''

    err_msg = "ERROR: destination directory is also listed among source directories! Please fix the config file."
    assert not any( src_dir.samefile(CFG['destination']) for src_dir in CFG['sources'] ), err_msg

    for role_dir in CFG['sources'] + [ CFG['destination'] ]:
        assert role_dir.is_dir(), f"ERROR: role directory '{role_dir}' does not exist! Please create it or fix the config file."
        for pypeline_dir in expand_directory_structure( role_dir ):
            if pypeline_dir.is_dir():
                continue
            LOG.info("Creating directory %s", pypeline_dir)
            pypeline_dir.mkdir(parents=True, exist_ok=True)


def display_role_paths() -> None:
    ''' Ensures that role (source/destination) directories actually exist, 
    that there is no incoherence (destination dir one of source dir) and
    creates pypeline directory structure.
    Raise AssertionError on directory not found or incoherence detected
    '''

    LOG.info("Currenty existing Pypeline directories")
    for role_dir in CFG['sources'] + [ CFG['destination'] ]:
        if not role_dir.is_dir():
            continue

        print( '  '*len(role_dir.parents) + '# ' + role_dir.name )

        dirs_to_display = sorted(
            expand_directory_structure( role_dir ),
            key=lambda x: len(str(x))
        ) 
        for pypeline_dir in dirs_to_display:
            if not pypeline_dir.is_dir():
                continue
            print( '  '*len(pypeline_dir.parents) + '> ' + pypeline_dir.name )


def remove_pypeline_directories() -> None:
    ''' Removes Pypeline's working directories
    '''
    for role_dir in CFG['sources'] + [ CFG['destination'] ]:
        if not role_dir.is_dir():
            LOG.info("Role directory '%s' does not exist! Please create it or fix the config file.", role_dir)
            continue
        
        dirs_to_remove = sorted(
            expand_directory_structure( role_dir ),
            key=lambda x: len(str(x)),
            reverse=True
        )
        
        for pypeline_dir in dirs_to_remove:
            if not pypeline_dir.is_dir():
                continue

            try:
                pypeline_dir.rmdir()
                LOG.info("Removed directory %s", pypeline_dir)
            except OSError:
                LOG.warning("Could not remove %s. It may not empty.", pypeline_dir)


def corresponding_destination_directory( role_dir: Path, src_dir: Path ) -> Path:
    ''' returns corresponding 'destination' directory path from Pypeline
    'source' directory path
    '''
    return CFG['destination'] / src_dir.relative_to( role_dir )



def activate_pipeline() -> None:
    ''' Scans source directories for content, moves it into
    destination directory. 
    '''

    def activate_pipeline_on_dirs( dir_couples: List[Path] ) -> None:

        _src_dirs = { dir_couple[0] for dir_couple in dir_couples }
        
        for dir_couple in dir_couples:
            _src_dir, _dst_dir = dir_couple

            for src_item in _src_dir.glob('*'):
                if src_item in _src_dirs:
                    # disallows recursive execution
                    continue 

                # Move operation: find unused path for destination, then move item
                _dest = find_available_path(
                    root=_dst_dir,
                    base_name=src_item.name,
                    file=src_item.is_file()
                )
                LOG.info("Moving %s -> %s", src_item, _dest)
                src_item.rename( _dest )


    for role_dir in CFG['sources']:
        if not role_dir.is_dir():
            continue

        LOG.info("Pipeline activated on %s", role_dir)

        dir_couples = [
            (src_dir, corresponding_destination_directory(role_dir, src_dir))
            for src_dir in expand_directory_structure( role_dir )
        ]
        activate_pipeline_on_dirs( dir_couples )



def main() -> None:
    ''' Main '''
    _cli_args = cli_args()
    read_config()
    print_cfg()
    display_role_paths()
    
    if _cli_args.remove_directories:
        remove_pypeline_directories()
        display_role_paths()
        return

    ensure_role_paths()
    display_role_paths()

    activate_pipeline()
    


if __name__=='__main__':
    
    main()
    print("END OF PROGRAM")
