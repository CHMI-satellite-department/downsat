def test_inject_parent() -> None:

    from abc import ABC

    from downsat.etl.metaclasses import inject_class_base

    class TestClass:
        ...

    class A(ABC):
        ...

    ModifiedClass = inject_class_base(TestClass, A)

    assert issubclass(ModifiedClass, A)


def test_update_class_dict() -> None:
    from downsat.etl.metaclasses import update_class_dict

    class TestClass:
        attr: int = 5

        def test(self) -> int:
            return self.attr + 1

    UpdatedClass = update_class_dict(TestClass, {"test": lambda s: s.attr - 1})

    assert UpdatedClass().test() == 4
