#!/bin/bash
export PYTHONPATH=/home/marvin/control:/home/marvin/control/3rdparty:/home/marvin/build/control/lib:$PYTHONPATH
export CTRL_ROOT=/home/marvin/control
export CTRL_EXPTS=/home/marvin/control/experiments2
export HDF5_DISABLE_VERSION_CHECK=1

cd /home/marvin/control/parRL
python startup.py

