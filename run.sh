#!/bin/bash
make clean; make
bash ./bcast_.sh
mpiexec -n 2 -f ./host ./main
