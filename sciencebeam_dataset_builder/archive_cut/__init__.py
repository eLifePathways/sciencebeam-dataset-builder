"""Cutting a corpus out of an archive of ranked, stratified documents.

Selection is stratified and nested: each version's splits contain the previous version's,
so numbers published against an earlier version stay true of a later one. Nothing here is
tied to a particular corpus — the stratum column, split names and counts all arrive as
configuration — but that is a consequence of the invariant needing a single
implementation, not a claim to serve corpora it has never seen.
"""
