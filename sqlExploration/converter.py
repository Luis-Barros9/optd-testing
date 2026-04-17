#!/usr/bin/env python3

"""Convert OPTD logical groups into pop-style memo inserts.

Current scope:
- LogicalGet  -> scan
- LogicalJoin -> join
- LogicalSelect -> filter
- Other operations not supported yet
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path


INSERT_RE = re.compile(
	r"insert\s+into\s+(?P<table>\w+)\s*\((?P<columns>.*?)\)\s*values\s*(?P<values>.*?);",
	re.IGNORECASE | re.DOTALL,
)


@dataclass
class ScalarNode:
	kind: str
	metadata: str | None = None
	children: list[tuple[int, int]] = field(default_factory=list)


def split_top_level(text: str, delimiter: str = ",") -> list[str]:
	"""Split text by delimiter while respecting quotes and nested braces/parens."""
	items: list[str] = []
	buf: list[str] = []
	in_quote = False
	depth_paren = 0
	depth_bracket = 0
	depth_brace = 0
	i = 0

	while i < len(text):
		ch = text[i]

		if ch == "'":
			if in_quote and i + 1 < len(text) and text[i + 1] == "'":
				buf.append("''")
				i += 2
				continue
			in_quote = not in_quote
			buf.append(ch)
			i += 1
			continue

		if not in_quote:
			if ch == "(":
				depth_paren += 1
			elif ch == ")":
				depth_paren -= 1
			elif ch == "[":
				depth_bracket += 1
			elif ch == "]":
				depth_bracket -= 1
			elif ch == "{":
				depth_brace += 1
			elif ch == "}":
				depth_brace -= 1

			if (
				ch == delimiter
				and depth_paren == 0
				and depth_bracket == 0
				and depth_brace == 0
			):
				items.append("".join(buf).strip())
				buf.clear()
				i += 1
				continue

		buf.append(ch)
		i += 1

	tail = "".join(buf).strip()
	if tail:
		items.append(tail)
	return items


def parse_insert_rows(values_text: str) -> list[list[str]]:
	"""Extract tuples from VALUES (...), (...); and return raw field strings."""
	rows: list[list[str]] = []
	i = 0
	while i < len(values_text):
		if values_text[i] != "(":
			i += 1
			continue

		start = i + 1
		in_quote = False
		depth = 1
		i += 1
		while i < len(values_text) and depth > 0:
			ch = values_text[i]
			if ch == "'":
				if in_quote and i + 1 < len(values_text) and values_text[i + 1] == "'":
					i += 2
					continue
				in_quote = not in_quote
			elif not in_quote:
				if ch == "(":
					depth += 1
				elif ch == ")":
					depth -= 1
					if depth == 0:
						row_text = values_text[start:i]
						rows.append(split_top_level(row_text, ","))
						break
			i += 1
	return rows


def parse_sql_literal(raw: str):
	raw = raw.strip()
	if raw.lower() == "null":
		return None
	if len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'":
		return raw[1:-1].replace("''", "'")
	if re.fullmatch(r"-?\d+", raw):
		return int(raw)
	if re.fullmatch(r"-?\d+\.\d+", raw):
		return float(raw)
	return raw


def parse_sql_file(sql_text: str) -> dict[str, list[dict[str, object]]]:
	by_table: dict[str, list[dict[str, object]]] = {}

	for m in INSERT_RE.finditer(sql_text):
		table = m.group("table").lower()
		columns = [c.strip() for c in m.group("columns").split(",")]
		rows = parse_insert_rows(m.group("values"))

		dest = by_table.setdefault(table, [])
		for row in rows:
			if len(row) != len(columns):
				continue
			parsed = {col: parse_sql_literal(val) for col, val in zip(columns, row)}
			dest.append(parsed)

	return by_table


def parse_columns_list(columns_value: object) -> list[int]:
	if columns_value is None:
		return []
	text = str(columns_value).strip()
	if not text:
		return []
	return [int(x.strip()) for x in text.split(",") if x.strip()]


def column_from_metadata(metadata: str | None) -> int | None:
	if not metadata:
		return None
	m = re.search(r"column\s*:\s*(-?\d+)", metadata)
	if not m:
		return None
	return int(m.group(1))


def collect_column_refs(root_id: int, nodes: dict[int, ScalarNode]) -> list[int]:
	result: list[int] = []
	seen: set[int] = set()

	def dfs(node_id: int) -> None:
		if node_id in seen:
			return
		seen.add(node_id)

		node = nodes.get(node_id)
		if not node:
			return

		if node.kind == "ColumnRef":
			col = column_from_metadata(node.metadata)
			if col is not None:
				result.append(col)

		for _, child_id in sorted(node.children, key=lambda p: p[0]):
			dfs(child_id)

	dfs(root_id)

	unique_ordered: list[int] = []
	seen_cols: set[int] = set()
	for c in result:
		if c not in seen_cols:
			seen_cols.add(c)
			unique_ordered.append(c)
	return unique_ordered


def format_int_array(values: list[int]) -> str:
	if not values:
		return "array[]::int[]"
	return "array[" + ",".join(str(v) for v in values) + "]"


def convert(optd_sql: str) -> list[str]:
	tables = parse_sql_file(optd_sql)

	groups = tables.get("group", [])
	expression_inputs = tables.get("expression_input", [])
	expression_scalars = tables.get("expression_scalar", [])
	scalar_rows = tables.get("scalar", [])

	scalar_nodes: dict[int, ScalarNode] = {}
	for row in scalar_rows:
		sid = int(row["id"])
		node = scalar_nodes.setdefault(
			sid,
			ScalarNode(kind=str(row.get("kind") or ""), metadata=row.get("metadata")),
		)
		node.kind = str(row.get("kind") or node.kind)
		node.metadata = row.get("metadata") if row.get("metadata") is not None else node.metadata

		parent = row.get("parent_scalar")
		position = row.get("position")
		if parent is not None and position is not None:
			parent_node = scalar_nodes.setdefault(int(parent), ScalarNode(kind=""))
			parent_node.children.append((int(position), sid))

	inputs_by_expr: dict[int, dict[int, int]] = {}
	for row in expression_inputs:
		expr_id = int(row["expression_id"])
		pos = int(row["position"])
		grp = int(row["input_group"])
		inputs_by_expr.setdefault(expr_id, {})[pos] = grp

	groups_by_id: dict[int, dict[str, object]] = {
		int(row["id"]): row for row in groups if row.get("id") is not None
	}

	scalar_roots_by_expr: dict[int, list[int]] = {}
	for row in expression_scalars:
		expr_id = int(row["expression_id"])
		scalar_id = int(row["scalar_id"])
		scalar_roots_by_expr.setdefault(expr_id, []).append(scalar_id)

	def collect_expression_column_refs(expr_id: int) -> list[int]:
		"""Collect all ColumnRef columns used by an expression's scalar roots."""
		col_refs: list[int] = []
		for scalar_root in scalar_roots_by_expr.get(expr_id, []):
			col_refs.extend(collect_column_refs(scalar_root, scalar_nodes))
		return list(dict.fromkeys(col_refs))

	memo_rows: list[str] = []
	memo_rows.append("  (0, 'noop', 0, 0, array[]::int[], array[]::int[]) ")
	for row in sorted(groups, key=lambda r: int(r["id"])):
		gid = int(row["id"])
		kind = str(row.get("kind") or "")
		cols = parse_columns_list(row.get("columns"))

		if kind == "LogicalGet":
			memo_rows.append(
				f"  ({gid}, 'scan', 0, 0, {format_int_array(cols)}, array[]::int[])"
			)
			continue

		if kind == "LogicalSelect":
			inputs = inputs_by_expr.get(gid, {})
			lchild = inputs.get(0)
			if lchild is None:
				continue
			exp_cols = collect_expression_column_refs(gid)
			memo_rows.append(
				f"  ({gid}, 'filter', {lchild}, 0, {format_int_array(cols)}, {format_int_array(exp_cols)})"
			)
			continue

		if kind == "LogicalProject":
			inputs = inputs_by_expr.get(gid, {})
			lchild = inputs.get(0)
			if lchild is None:
				continue
			memo_rows.append(
				f"  ({gid}, 'proj', {lchild}, 0, {format_int_array(cols)}, {format_int_array(cols)})"
			)
			continue

		if kind != "LogicalJoin":
			continue

		inputs = inputs_by_expr.get(gid, {})
		lchild = inputs.get(0)
		rchild = inputs.get(1)
		if lchild is None or rchild is None:
			continue

		exp_cols: list[int] = []
		for scalar_root in scalar_roots_by_expr.get(gid, []):
			exp_cols.extend(collect_column_refs(scalar_root, scalar_nodes))
		# Keep deterministic unique order.
		exp_cols = list(dict.fromkeys(exp_cols))

		memo_rows.append(
			f"  ({gid}, 'join', {lchild}, {rchild}, {format_int_array(cols)}, {format_int_array(exp_cols)})"
		)

	return memo_rows


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Convert OPTD logical SQL group inserts into pop-style memo inserts"
	)
	parser.add_argument(
		"input",
		nargs="?",
		type=Path,
		default=Path("./optdLogical.sql"),
		help="Input SQL file (defaults to ./optdLogical.sql)",
	)
	parser.add_argument(
		"-o",
		"--output",
		type=Path,
		default=None,
		help="Optional output file path. If omitted, prints to stdout.",
	)
	args = parser.parse_args()

	sql_text = args.input.read_text(encoding="utf-8")
	rows = convert(sql_text)

	lines: list[str] = []
	lines.append("insert into memo values")
	if rows:
		for idx, row in enumerate(rows):
			suffix = "," if idx < len(rows) - 1 else ";"
			lines.append(row + suffix)
	else:
		lines.append("    -- no LogicalGet/LogicalSelect/LogicalJoin rows found;")

	output = "\n".join(lines) + "\n"

	if args.output is None:
		print(output, end="")
	else:
		args.output.write_text(output, encoding="utf-8")


if __name__ == "__main__":
	main()
