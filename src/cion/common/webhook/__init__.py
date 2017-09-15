from cion.common.workq.interface import Interface

webhook = Interface("webhook")


@webhook.task
def hello(name):
    pass
