#!/bin/bash

cmd_args=$(python -m files_handler --input $@)
/docker/entrypoint.sh $cmd_args
python -m files_handler --output $@
