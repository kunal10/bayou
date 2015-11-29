#!/bin/bash

javac ut/distcomp/framework/*.java ut/distcomp/bayou/*.java
python tester.py
./cleanFolder.sh