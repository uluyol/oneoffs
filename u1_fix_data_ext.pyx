# Compiled part of u1-data-to-jsonlines.py
#
# This file is compiled with Cython for performance,
# but even still things are pretty slow.
# Bottleneck is probably reading CSV or encoding JSON.
# Second can be sped up with a lot of ugliness.
# First is hard since the CSV has complexities like
# commas inside quoted strings.
#
# Runs on my machine over the data in less than a day,
# so good enough I guess.

import ast

fix_toint = [
	"size",
	"shares",
	"pid",
	"free_bytes",
	"req_id",
	"hash",
	"node_id",
	"user_id",
	"root",
	"udfs",
	"vol_id",
	"current_gen",
	"from_gen",
	"nodes",
	"user",
	"shared_by",
	"shared_to",
]

cdef object fix_float(str t):
	if t:
		return str(t)
	return -1.0

cdef str fix_str(str t):
	if t:
		if t == "None":
			return ""
		return ast.literal_eval(t)
	return ""

cdef object fix_dict(str t):
	if t:
		if t == "None":
			return {}
		return dict(ast.literal_eval(t))
	else:
		return {}

def load_and_fix(obj, row):
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

	cdef str key, t
	for key in fix_toint:
		t = obj[key]
		if t:
			obj[key] = int(t)
		else:
			obj[key] = -1

	obj["time"] = fix_float(obj["time"])

	obj["ext"] = fix_str(obj["ext"])
	obj["mime"] = fix_str(obj["mime"])

	obj["client_metadata"] = fix_dict(obj["client_metadata"])

	if obj["caps"].startswith("frozenset("):
		t = obj["caps"][len("frozenset("):-1]
		obj["caps"] = ast.literal_eval(t)
		obj["caps"].sort()
	elif obj["caps"] == "":
		obj["caps"] = []
	else:
		raise ValueError("caps is not empty and is not frozenset: " + obj["caps"])
