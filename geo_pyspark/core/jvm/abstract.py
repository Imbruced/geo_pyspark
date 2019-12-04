from abc import ABC

import attr


@attr.s
class JvmObject(ABC):

    jvm = attr.ib()

    @property
    def jvm_reference(self):
        if self.jvm is None:
            raise AttributeError("Jvm object can not be found")
        raise NotImplemented("Jvm Object has to implement jvm_reference method")

    def create_jvm_instance(self):
        raise NotImplemented("Instance has to implement create_jvm_instance")

    @property
    def jvm_instance(self):
        return self.create_jvm_instance()
