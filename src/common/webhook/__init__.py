from common.workq.task import Interface

interface = Interface("webhook")


@interface.task
def hello(name):
    pass