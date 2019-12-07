from abc import ABC

import attr


@attr.s
class JvmObject(ABC):

    jvm = attr.ib()

    @property
    def jvm_reference(self):
        if self.jvm is None:
            raise AttributeError("Jvm object can not be found")

    def get_reference(self):
        return getattr(self.jvm, self.jvm_reference)

    def create_jvm_instance(self):
        raise NotImplemented("Instance has to implement create_jvm_instance")

    @property
    def jvm_instance(self):
        return self.create_jvm_instance()
