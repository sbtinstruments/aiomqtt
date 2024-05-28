from __future__ import annotations

import pytest

from aiomqtt import Topic, Wildcard


def test_topic_validation() -> None:
    """Test that Topic raises Exceptions for invalid topics."""
    with pytest.raises(TypeError):
        Topic(True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Topic([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a/b/#")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a/+/c")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("#")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("")
    with pytest.raises(ValueError, match="Invalid topic: "):
        Topic("a" * 65536)


def test_wildcard_validation() -> None:
    """Test that Wildcard raises Exceptions for invalid wildcards."""
    with pytest.raises(TypeError):
        Wildcard(True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Wildcard([])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/#/c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/b+/c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a/b/#c")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("")
    with pytest.raises(ValueError, match="Invalid wildcard: "):
        Wildcard("a" * 65536)


def test_topic_matches() -> None:
    """Test that Topic.matches() does and doesn't match some test wildcards."""
    topic = Topic("a/b/c")
    assert topic.matches("a/b/c")
    assert topic.matches("a/+/c")
    assert topic.matches("+/+/+")
    assert topic.matches("+/#")
    assert topic.matches("#")
    assert topic.matches("a/b/c/#")
    assert topic.matches("$share/group/a/b/c")
    assert topic.matches("$share/group/a/b/+")
    assert not topic.matches("abc")
    assert not topic.matches("a/b")
    assert not topic.matches("a/b/c/d")
    assert not topic.matches("a/b/c/d/#")
    assert not topic.matches("a/b/z")
    assert not topic.matches("a/b/c/+")
    assert not topic.matches("$share/a/b/c")
    assert not topic.matches("$test/group/a/b/c")

def test_dollar_matches() -> None:
    """Test that Topic.matches() does and doesn't match some test wildcards."""
    topic = Topic("a/$foo/c")
    assert topic.matches("#")
    assert topic.matches("a/$foo/#")
    assert topic.matches("a/$foo/c")
    assert topic.matches("a/$foo/c/#")
    assert topic.matches("$share/group/a/$foo/c")

    assert not topic.matches("a/b/c")
    assert not topic.matches("a/+/c")
    assert not topic.matches("+/+/+")
    assert not topic.matches("+/#")
    assert not topic.matches("a/$foo/c/d")
    assert not topic.matches("$share/group/a/b/c")
