"""
Auto-generated class for DigitalAssetAddress
"""
from .Label import Label

from . import client_support


class DigitalAssetAddress(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(address, currencysymbol, expire, label, noexpiration=None):
        """
        :type address: str
        :type currencysymbol: str
        :type expire: datetime
        :type label: Label
        :type noexpiration: bool
        :rtype: DigitalAssetAddress
        """

        return DigitalAssetAddress(
            address=address,
            currencysymbol=currencysymbol,
            expire=expire,
            label=label,
            noexpiration=noexpiration,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'DigitalAssetAddress'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'address'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.address = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'currencysymbol'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.currencysymbol = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'expire'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.expire = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'label'
        val = data.get(property_name)
        if val is not None:
            datatypes = [Label]
            try:
                self.label = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'noexpiration'
        val = data.get(property_name)
        if val is not None:
            datatypes = [bool]
            try:
                self.noexpiration = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
