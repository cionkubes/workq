from workq.interface import Interface

service = Interface("service")


@service.task
def update(service, image):
    pass
