#!/bin/bash
#conda init --all --dry-run --verbose
conda activate python3
python3 traffic_predict.py
conda deactivate