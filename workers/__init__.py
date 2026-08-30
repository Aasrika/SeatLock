"""Background jobs that run as their own OS process, sharing the modular
monolith's one database -- not separate services (CLAUDE.md rule 1).

sweeper.py: reclaims expired holds (SPEC.md section 5, invariant I3).
reconciler.py: repairs Redis/Postgres divergence in the hold mirror
(SPEC.md section 5).
"""
