import hashlib


class Signature:
    @property
    def signature(self):
        m = hashlib.md5()

        for sig in self.signature_generator():
            m.update(sig)

        return m.hexdigest()

    def signature_generator(self):
        raise NotImplementedError()