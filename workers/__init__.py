"""Background jobs that run as their own OS process, sharing the modular
monolith's one database -- not separate services (CLAUDE.md rule 1).

Only the hold sweeper exists so far (sweeper_worker.py). SPEC.md section 2
also names a reconciler here for a later phase.
"""
