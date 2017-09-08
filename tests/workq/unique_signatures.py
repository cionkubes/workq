import pytest

from cion.common.workq.task import Interface


@pytest.fixture
def interface1():
    interface = Interface("test1")

    @interface.task
    def test():
        pass

    return interface


@pytest.fixture
def interface2():
    interface = Interface("test2")

    @interface.task
    def no_arg():
        pass

    return interface


def test_interface_signature_changes_with_added_tasks(interface1):
    sig1 = interface1.signature()

    @interface1.task
    def test2():
        pass

    assert sig1 != interface1.signature()


def test_unique_interfaces(interface1, interface2):
    assert interface1.signature() != interface2.signature()


def test_same_task_different_interfaces(interface1, interface2):
    def test2():
        pass

    interface1.task(test2)
    interface2.task(test2)

    assert interface1.test2.signature() != interface2.test2.signature()