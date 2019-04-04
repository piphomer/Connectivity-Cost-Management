"""
Run to complete common tasks for all support scripts.

Current actions:
    - Print script start time
    - Add ```support``` package to sys.path

Copy this file into any directory in `scripts` to make it available.
"""

# Print script start date and time
import datetime as dt
now = dt.datetime.now()
print dt.datetime.strftime(now, "%d-%b-%Y %H:%M:%S") + "\n"


# Add parent directory of ```support``` package to sys.path to allow
# modules from the ```support``` python package to be imported.
import sys
repo = "SMART-Solar-Support"
pkg_dir = __file__[0:__file__.find(repo)+len(repo)]
sys.path.append(pkg_dir)
