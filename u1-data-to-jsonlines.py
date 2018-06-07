#!/usr/bin/env python3

import ast
import copy
import csv
import json
import sys

for row in csv.reader(sys.stdin):
	for idx, default in ((3, dict()), (5, ""), (13, "")):
		if row[idx]:
			row[idx] = ast.literal_eval(row[idx])
		else:
			row[idx] = copy.copy(default)
	if row[2].startswith("frozenset("):
		row[2] = row[2][len("frozenset("):-1]
		row[2] = ast.literal_eval(row[2])
	elif row[2] == "":
		row[2] = []
	else:
		raise ValueError("column 3 is not empty and is not frozenset: " + row[2])
	print(row)
