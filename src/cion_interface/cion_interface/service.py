from workq.interface import Interface

service = Interface("service")


@service.task
def update(swarm, service, image):
    pass


@service.task
def distribute_to(image):
    pass
