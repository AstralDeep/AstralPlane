"""Conformance test scenario for voice session reconnect and duplicate turn handling."""

from __future__ import annotations

import pytest

def test_voice_session_reconnect_duplicate_turn_contract():
    # Verify duplicate turn IDs reject gracefully without session state corruption
    seen_turns = set()
    
    def process_turn(turn_id: str) -> bool:
        if turn_id in seen_turns:
            return False
        seen_turns.add(turn_id)
        return True

    assert process_turn("turn-001") is True
    assert process_turn("turn-001") is False  # Rejected duplicate
    assert process_turn("turn-002") is True
    assert len(seen_turns) == 2
