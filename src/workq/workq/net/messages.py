class Types:
    SUPPORTS = 0
    RESPONSE = 1
    DO_WORK = 2
    WORK_COMPLETE = 3
    PING = 4


class Keys:
    WORK_ID = 'w'
    WORK_RESULT = 'r'
    WORK_EXC = 'x'
    KWARGS = 'd'
    ARGS = 'a'
    TASK = 'T'
    TYPE = "t"
    INTERFACE = 'i'
    ERROR = 'e'
    MSG = 'm'


ping = { Keys.TYPE: Types.PING }


def ok(**data):
    return {Keys.TYPE: Types.RESPONSE, Keys.ERROR: False, **data}


def error(msg):
    return {Keys.TYPE: Types.RESPONSE, Keys.ERROR: True, Keys.MSG: msg}


def supports_interface(interface):
    return {Keys.TYPE: Types.SUPPORTS, Keys.INTERFACE: interface.signature}


def start_work(work_id, task, args, kwargs):
    return {
        Keys.TYPE: Types.DO_WORK,
        Keys.TASK: task.signature,
        Keys.WORK_ID: work_id,
        Keys.ARGS: args,
        Keys.KWARGS: kwargs
    }


def work_result(work_id, result):
    return {
        Keys.TYPE: Types.WORK_COMPLETE,
        Keys.WORK_ID: work_id,
        Keys.WORK_RESULT: result
    }


def work_failed(work_id, exception):
    return {
        Keys.TYPE: Types.WORK_COMPLETE,
        Keys.WORK_ID: work_id,
        Keys.WORK_EXC: exception
    }


def error_guard(response):
    assert response[Keys.TYPE] == Types.RESPONSE, "Expected response type"
    assert not response[Keys.ERROR], response[Keys.MSG]


def expect_guard(type, response):
    assert response[Keys.TYPE] == type, f"Expected {type} type, but got {response[Keys.TYPE]}."
