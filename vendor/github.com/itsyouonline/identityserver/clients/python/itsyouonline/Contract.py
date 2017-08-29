"""
Auto-generated class for Contract
"""
from .Party import Party
from .Signature import Signature

from . import client_support


class Contract(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(content, contractId, contractType, expires, parties, signatures, extends=None, invalidates=None):
        """
        :type content: str
        :type contractId: str
        :type contractType: str
        :type expires: datetime
        :type extends: list[str]
        :type invalidates: list[str]
        :type parties: list[Party]
        :type signatures: list[Signature]
        :rtype: Contract
        """

        return Contract(
            content=content,
            contractId=contractId,
            contractType=contractType,
            expires=expires,
            extends=extends,
            invalidates=invalidates,
            parties=parties,
            signatures=signatures,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'Contract'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'content'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.content = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'contractId'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.contractId = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'contractType'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.contractType = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'expires'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.expires = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'extends'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.extends = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'invalidates'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.invalidates = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'parties'
        val = data.get(property_name)
        if val is not None:
            datatypes = [Party]
            try:
                self.parties = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'signatures'
        val = data.get(property_name)
        if val is not None:
            datatypes = [Signature]
            try:
                self.signatures = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
