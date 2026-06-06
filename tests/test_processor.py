"""Tests for StreamMind stream processor"""
import pytest
from unittest.mock import MagicMock, patch

def test_event_structure():
    event = {"id": "abc", "timestamp": 1234.5, "text": "hello world", "source": "test"}
    assert "text" in event
    assert isinstance(event["timestamp"], float)

def test_vector_dimension():
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    emb = model.encode("test text")
    assert emb.shape == (384,)
