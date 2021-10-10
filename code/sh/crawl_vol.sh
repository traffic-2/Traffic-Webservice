#!/bin/bash
#conda init --all --dry-run --verbose
conda activate python3
python3 crawl_vol.py
conda deactivate