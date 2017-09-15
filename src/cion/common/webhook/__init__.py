from cion.common.workq.task import Interface

webhook = Interface("webhook")


@webhook.task
def hello(name):
    pass
