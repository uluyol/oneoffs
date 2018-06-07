#!/usr/bin/env python3
#
# Convert Ubuntu One data from http://cloudspaces.eu/results/datasets
# into a json lines file. The download link on the above site is dead but
# http://ast-deim.urv.cat/sample_u1_trace/ubuntuone_trace.gz
#
# Expected usage:
# zcat ubuntuone_trace.gz | ./u1-data-to-jsonlines.py | gzip > ubuntuone_json.gz

import ast
import copy
import csv
import json
import sys

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

fix_rules = [
	("client_metadata", dict(), dict),
	("ext", "", str),
	("mime", "", str),
	("size", -1, int),
	("shares", -1, int),
	("time", -1.0, float),
	("pid", -1, int),
	("free_bytes", -1, int),
	("req_id", -1, int),
	("hash", -1, int),
	("node_id", -1, int),
	("user_id", -1, int),
	("root", -1, int),
	("udfs", -1, int),
	("vol_id", -1, int),
	("current_gen", -1, int),
	("from_gen", -1, int),
	("nodes", -1, int),
	("user", -1, int),
	("shared_by", -1, int),
	("shared_to", -1, int),
]

for row in csv.reader(sys.stdin):	
	obj["T"] = row[0]
	obj["addr"] = row[1]
	obj["caps"] = row[2]
	obj["client_metadata"] = row[3]
	obj["current_gen"] = row[4]
	obj["ext"] = row[5]
	obj["failed"] = row[6]
	obj["free_bytes"] = row[7]
	obj["from_gen"] = row[8]
	obj["hash"] = row[9]
	obj["level"] = row[10]
	obj["logfile_id"] = row[11]
	obj["method"] = row[12]
	obj["mime"] = row[13]
	obj["msg"] = row[14]
	obj["node_id"] = row[15]
	obj["nodes"] = row[16]
	obj["pid"] = row[17]
	obj["req_id"] = row[18]
	obj["req_t"] = row[19]
	obj["root"] = row[20]
	obj["server"] = row[21]
	obj["shared_by"] = row[22]
	obj["shared_to"] = row[23]
	obj["shares"] = row[24]
	obj["sid"] = row[25]
	obj["size"] = row[26]
	obj["time"] = row[27]
	obj["tstamp"] = row[28]
	obj["type"] = row[29]
	obj["udfs"] = row[30]
	obj["user_id"] = row[31]
	obj["user"] = row[32]
	obj["vol_id"] = row[33]

	for key, default, type in fix_rules:
		if obj[key]:
			obj[key] = type(ast.literal_eval(obj[key]))
		else:
			obj[key] = copy.copy(default)
	if obj["caps"].startswith("frozenset("):
		t = obj["caps"][len("frozenset("):-1]
		obj["caps"] = ast.literal_eval(t)
		obj["caps"].sort()
	elif obj["caps"] == "":
		obj["caps"] = []
	else:
		raise ValueError("caps is not empty and is not frozenset: " + obj["caps"])

	try:
		sys.stdout.write(json.dumps(obj, sort_keys=True))
		sys.stdout.write("\n")
	except BrokenPipeError:
		break
