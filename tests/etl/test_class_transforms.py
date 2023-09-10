def test_multikey_ds_on_class() -> None:

    from collections import UserDict

    from downsat.etl import abc, protocols
    from downsat.etl.class_transforms import multikey_ds

    @multikey_ds
    class DictLike(UserDict):
        ...

    container = DictLike(a=1, b=2)

    # single-key getitem works
    assert container["a"] == 1

    # multi-key getitem works
    assert container["a", "b"] == (1, 2)

    # DictLike fufills protocol MultiKeyDataSource
    assert isinstance(container, protocols.MultiKeyDataSource)

    # DictLike is an ancestor of MultiKeyDataSource
    assert isinstance(container, abc.MultiKeyDataSource)
