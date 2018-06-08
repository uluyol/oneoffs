#!/usr/bin/env python3
#
# Convert Ubuntu One data from http://cloudspaces.eu/results/datasets
# into a json lines file. The download link on the above site is dead but
# http://ast-deim.urv.cat/sample_u1_trace/ubuntuone_trace.gz
#
# Expected usage:
# zcat ubuntuone_trace.gz | ./u1-data-to-jsonlines.py | gzip > ubuntuone_json.gz

import ast
import csv
import sys

from concurrent import futures

import simplejson as json

import pyximport; pyximport.install()
import u1_fix_data_ext

def tojson(lines):
	obj = {
		"T": None,
		"addr": None,
		"caps": None,
		"client_metadata": None,
		"current_gen": None,
		"ext": None,
		"failed": None,
		"free_bytes": None,
		"from_gen": None,
		"hash": None,
		"level": None,
		"logfile_id": None,
		"method": None,
		"mime": None,
		"msg": None,
		"node_id": None,
		"nodes": None,
		"pid": None,
		"req_id": None,
		"req_t": None,
		"root": None,
		"server": None,
		"shared_by": None,
		"shared_to": None,
		"shares": None,
		"sid": None,
		"size": None,
		"time": None,
		"tstamp": None,
		"type": None,
		"udfs": None,
		"user_id": None,
		"user": None,
		"vol_id": None,
	}

	out = []
	for row in csv.reader(lines):	
		u1_fix_data_ext.load_and_fix(obj, row)
		out.append(json.dumps(obj, sort_keys=True) + "\n")
	return out

def chunklines(lines, csize):
	ll = []
	l = []
	pos = 0
	while pos < len(lines):
		for i in range(csize):
			l.append(lines[pos])
			pos += 1
			if pos >= len(lines):
				break
		ll.append(l)
		l = []
	return ll

with futures.ProcessPoolExecutor() as exec:
	done = False
	while not done:
		lines = []
		n = 0
		for line in sys.stdin:
			lines.append(line)
			n += 1
			if n > 50000:
				break
		done = n == 0

		chunks = chunklines(lines, 500)

		jll = exec.map(tojson, chunks)
		for jl in jll:
			for l in jl:
				try:
					sys.stdout.write(l)
				except BrokenPipeError:
					sys.exit(0)
