def test_extract_fields_from_partial() -> None:
    """Test that _extract_fields can handle partial functions."""
    from functools import partial

    from attrs import field, frozen

    from downsat.etl.class_utils import _extract_fields

    @frozen
    class A:
        a: int = field()
        b: int = field()

    B = partial(A, a=1)

    fields = _extract_fields(B)
    assert len(fields) == 1
    assert "b" in fields
    assert "a" not in fields
