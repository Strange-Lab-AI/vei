"""vei.dynamics — learned enterprise dynamics subsystem.

This is a real in-repo module, not a contract-only boundary.  It absorbs
the existing benchmark_bridge trainer as its reference learned path.
External backends (e.g. ARP_Jepa_exp) plug in as optional adapters.

DynamicsBackend is the only surface other VEI modules use to call into
learned code.
"""
