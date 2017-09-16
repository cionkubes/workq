import pytest

from workq.interface import Interface


@pytest.fixture
def interface():
    interface = Interface("test")

    @interface.task
    def no_arg():
        pass

    @interface.task
    def args(*args):
        pass

    @interface.task
    def kwargs(**kwargs):
        pass

    @interface.task
    def trippel(arg, *args, **kwargs):
        pass

    @interface.task
    def arg1():
        pass

    @interface.task
    def arg2(arg1, arg2):
        pass

    return interface


def test_different_nr_args(interface):
    with pytest.raises(AssertionError):
        @interface.arg2.implement
        def implementation():
            pass

    with pytest.raises(AssertionError):
        @interface.arg2.implement
        def implementation(arg1):
            pass

    @interface.arg2.implement
    def implementation(arg1, arg2):
        pass

    with pytest.raises(AssertionError):
        @interface.arg2.implement
        def implementation(arg1, arg2, arg3):
            pass


def test_no_arg(interface):
    with pytest.raises(AssertionError):
        @interface.no_arg.implement
        def implementation(arg):
            pass

    @interface.no_arg.implement
    def implementation():
        pass


def test_args(interface):
    with pytest.raises(AssertionError):
        @interface.args.implement
        def implementation(arg):
            pass

    @interface.args.implement
    def implementation(*args):
        pass

    with pytest.raises(AssertionError):
        @interface.arg1.implement
        def implementation(*args):
            pass


def test_kwargs(interface):
    with pytest.raises(AssertionError):
        @interface.kwargs.implement
        def implementation(arg):
            pass

    @interface.kwargs.implement
    def implementation(**kwargs):
        pass

    with pytest.raises(AssertionError):
        @interface.arg1.implement
        def implementation(**kwargs):
            pass


def test_trippel(interface):
    @interface.trippel.implement
    def implementation(arg, *args, **kwargs):
        pass