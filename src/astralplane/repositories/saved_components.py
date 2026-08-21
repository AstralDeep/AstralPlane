"""Stable public name for publication-aware saved-component persistence.

The durable ``saved_components`` table is already owned by
:class:`~astralplane.repositories.workspaces.CanvasRepository`.  This module
publishes the required consumer-facing repository name without introducing a
second implementation or a divergent SQL path.
"""

from __future__ import annotations

from astralplane.repositories.workspaces import CanvasComponentRecord, CanvasRepository

SavedComponentRecord = CanvasComponentRecord


class SavedComponentRepository(CanvasRepository):
    """Publication-aware component storage under its stable catalog name."""


__all__ = ("SavedComponentRecord", "SavedComponentRepository")
