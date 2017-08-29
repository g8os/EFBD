"""
Auto-generated class for OrganizationAPIKey
"""
from .Label import Label

from . import client_support


class OrganizationAPIKey(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(label, callbackURL=None, clientCredentialsGrantType=None, secret=None):
        """
        :type callbackURL: str
        :type clientCredentialsGrantType: bool
        :type label: Label
        :type secret: str
        :rtype: OrganizationAPIKey
        """

        return OrganizationAPIKey(
            callbackURL=callbackURL,
            clientCredentialsGrantType=clientCredentialsGrantType,
            label=label,
            secret=secret,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'OrganizationAPIKey'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'callbackURL'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.callbackURL = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'clientCredentialsGrantType'
        val = data.get(property_name)
        if val is not None:
            datatypes = [bool]
            try:
                self.clientCredentialsGrantType = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

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

        property_name = 'secret'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.secret = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
