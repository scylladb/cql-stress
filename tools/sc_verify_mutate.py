#! /usr/bin/env python3

"""Injects one known anomaly into rows of a cql-stress-sc-verify check file (history v2).

Each kind in ILLEGAL rewrites one cell of one read so that no order of the row's operations
explains it; each kind in LEGAL changes the file in a way that keeps every row
linearizable. The choice is deterministic: the first eligible operations in file order, so a
failure reproduces. Writes are "acked" when their return says ok; a write without a return
has an unknown outcome and may land at any time after its call.

    sc_verify_mutate.py KIND < check_file.jsonl > mutated.jsonl
"""

import json
import sys

ILLEGAL = ("stale", "torn", "backwards", "future", "phantom")
LEGAL = ("identity", "drop-read", "unack-write", "shift")
KINDS = ILLEGAL + LEGAL


class Row:
    """One `# key` block: its header and its operations, call and return lines paired."""

    def __init__(self, header):
        self.header = header
        self.key = int(header.split()[2])
        self.lines = []      # parsed JSON lines, in file order
        self.dropped = set()  # indexes into lines left out of the output

    def ops(self):
        ops = {}
        for i, line in enumerate(self.lines):
            op = ops.setdefault(line["id"], {"id": line["id"], "op": line["op"]})
            op[line["kind"]] = i
        return list(ops.values())

    def call(self, op):
        return self.lines[op["call"]]

    def ret(self, op):
        return self.lines[op["return"]] if "return" in op else None

    def writes(self):
        """(wid, cells, call time, return time or None, acked) of every write."""
        out = []
        for op in self.ops():
            if op["op"] != "write":
                continue
            cells = self.call(op)["cells"]
            wid = next(c for c in cells if c is not None)
            ret = self.ret(op)
            out.append({"wid": wid, "cells": {i for i, c in enumerate(cells) if c is not None},
                        "call": self.call(op)["time_ns"], "ret": ret and ret["time_ns"],
                        "acked": ret is not None and ret["status"] == "ok", "op": op})
        return out

    def reads(self):
        """Reads that returned: (return line, call time, return time)."""
        out = []
        for op in self.ops():
            ret = self.ret(op)
            if op["op"] == "read" and ret is not None and ret["status"] == "ok":
                out.append({"line": ret, "call": self.call(op)["time_ns"], "ret": ret["time_ns"],
                            "op": op})
        return out


def parse(text):
    rows, head = [], []
    for raw in text.splitlines():
        if raw.startswith("# key"):
            rows.append(Row(raw))
        elif rows:
            rows[-1].lines.append(json.loads(raw))
        else:
            head.append(raw)
    return head, rows


def render(head, rows):
    out = list(head)
    for row in rows:
        out.append(row.header)
        out.extend(json.dumps(line, separators=(",", ":"))
                   for i, line in enumerate(row.lines) if i not in row.dropped)
    return "\n".join(out) + "\n"


def stale(row):
    """W1 and W2 are acked writes of cell c, W1 returned before W2 was called, and W2 returned
    before read R was called. R is changed to show W1 on c. W2 is applied before R starts and
    after W1, so R must see W2 or a later write on c, never W1."""
    writes = [w for w in row.writes() if w["acked"]]
    for r in row.reads():
        for c in range(len(r["line"]["cells"])):
            for w2 in writes:
                if c in w2["cells"] and w2["ret"] < r["call"]:
                    for w1 in writes:
                        if c in w1["cells"] and w1["ret"] < w2["call"]:
                            r["line"]["cells"][c] = w1["wid"]
                            return True
    return False


def torn(row):
    """As stale, but W2 wrote every cell and R still shows W2 on another cell d: R sees one
    half of W2 and not the other (a torn row). Since W2 is visible in R, so are all its cells."""
    writes = [w for w in row.writes() if w["acked"]]
    for r in row.reads():
        cells = r["line"]["cells"]
        for w2 in writes:
            if len(w2["cells"]) < len(cells) or w2["ret"] >= r["call"]:
                continue
            shown = [d for d, v in enumerate(cells) if v == w2["wid"]]
            for c in range(len(cells)):
                if not any(d != c for d in shown):
                    continue
                for w1 in writes:
                    if c in w1["cells"] and w1["ret"] < w2["call"]:
                        cells[c] = w1["wid"]
                        return True
    return False


def backwards(row):
    """Read R1 returned before read R2 was called, R1 shows X on cell c from write WX, and acked
    write W0 of c returned before WX was called. R2 is changed to show W0 on c. R1 proves WX
    is applied before R2 starts, and WX comes after W0, so R2 cannot see W0."""
    writes = row.writes()
    by_wid = {w["wid"]: w for w in writes}
    reads = row.reads()
    for r1 in reads:
        for c, x in enumerate(r1["line"]["cells"]):
            wx = by_wid.get(x)
            if wx is None:
                continue
            for w0 in writes:
                if w0["acked"] and c in w0["cells"] and w0["ret"] < wx["call"]:
                    for r2 in reads:
                        if r2["call"] > r1["ret"]:
                            r2["line"]["cells"][c] = w0["wid"]
                            return True
    return False


def future(row):
    """R is changed to show, on cell c, a write W of c that was called after R returned. No
    read can see a write issued after it ended."""
    writes = row.writes()
    for r in row.reads():
        for w in writes:
            if w["call"] > r["ret"]:
                r["line"]["cells"][min(w["cells"])] = w["wid"]
                return True
    return False


def phantom(row):
    """R is changed to show a write id that no write of the row issued."""
    reads = row.reads()
    if not reads:
        return False
    wids = [w["wid"] for w in row.writes()]
    reads[0]["line"]["cells"][0] = max(wids, default=0) + 1000
    return True


def identity(row):
    """Nothing changes."""
    return True


def drop_read(row):
    """A read is left out. A subset of a linearizable history's reads stays linearizable."""
    reads = row.reads()
    if not reads:
        return False
    op = reads[0]["op"]
    row.dropped |= {op["call"], op["return"]}
    return True


def unack_write(row):
    """An acked write's return is left out, so its outcome becomes unknown. That only removes
    a constraint: the same order still explains the row."""
    for w in row.writes():
        if w["acked"]:
            row.dropped.add(w["op"]["return"])
            return True
    return False


def shift(row):
    """Every time of the row moves by the same amount, which keeps every order."""
    for line in row.lines:
        line["time_ns"] += 1_000_000_000
    return True


MUTATIONS = {"stale": stale, "torn": torn, "backwards": backwards, "future": future,
             "phantom": phantom, "identity": identity, "drop-read": drop_read,
             "unack-write": unack_write, "shift": shift}


def mutate(text, kind, rows=5):
    """Applies KIND to the first `rows` rows where it is possible. Returns the new file and the
    keys of the rows it changed."""
    head, parsed = parse(text)
    changed = set()
    for row in parsed:
        if len(changed) == rows:
            break
        if MUTATIONS[kind](row):
            changed.add(row.key)
    return render(head, parsed), changed


if __name__ == "__main__":
    text, keys = mutate(sys.stdin.read(), sys.argv[1])
    sys.stdout.write(text)
    print(f"{sys.argv[1]}: rows {sorted(keys)}", file=sys.stderr)
