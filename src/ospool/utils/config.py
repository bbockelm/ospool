"""Tools for retrieving the user's configs for the ospool tools"""

import sqlite3
import os
import pwd
import pathlib


def _get_home_dir():
     home = os.environ.get("HOME")
     if home:
         return home

     return pwd.getpwuid(os.geteuid()).pw_dir


def _get_state_dir():
     state_base = pathlib.Path(os.environ.get("XDG_STATE_HOME", os.path.join(_get_home_dir(), ".local/state")))
     state_base /= "ospool"

     os.makedirs(state_base, mode=0o700, exist_ok=True)

     return state_base


def _get_state_db(read_only=False):
     if read_only:
         conn = sqlite3.connect("file:{}?mode=ro".format(_get_state_dir() / "state.db"), uri=True)
     else:
         conn = sqlite3.connect(str(_get_state_dir() / "state.db"))

     with conn:
         all_tables = set(row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))
         if 'pool_history' not in all_tables:
             conn.execute("CREATE TABLE pool_history (name text)")

     return conn


def get_pool_history():
    """
    Return a set of all pools used in the recorded history
    """
    try:
        conn = _get_state_db(read_only=True)
        with conn:
            return set(row[0] for row in conn.execute("SELECT name FROM pool_history"))
    except:
        return set()
    return all_pools


def add_pool_history(pool):
    """
    Record a pool has been seen in the 
    """
    # Opening a SQLite3 DB read-write requires a lock; these can be heavy for shared FS.
    # Since we will write to the DB only the first time the pool is used (and there aren't
    # that many pools around!), optimistically assume we can get away with read-only.
    if pool in get_pool_history():
        return

    with _get_state_db() as conn:
        conn.execute("INSERT INTO pool_history VALUES (?)", (pool, ));
