"""
Auto-generated class for BankAccount
"""
from .Label import Label

from . import client_support


class BankAccount(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(bic, country, iban, label):
        """
        :type bic: str
        :type country: str
        :type iban: str
        :type label: Label
        :rtype: BankAccount
        """

        return BankAccount(
            bic=bic,
            country=country,
            iban=iban,
            label=label,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'BankAccount'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'bic'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.bic = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'country'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.country = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'iban'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.iban = client_support.val_factory(val, datatypes)
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

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
