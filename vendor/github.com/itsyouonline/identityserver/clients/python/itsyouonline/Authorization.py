"""
Auto-generated class for Authorization
"""
from .AuthorizationMap import AuthorizationMap

from . import client_support


class Authorization(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(grantedTo, organizations, username, addresses=None, bankaccounts=None, emailaddresses=None, facebook=None, github=None, phonenumbers=None, publicKeys=None):
        """
        :type addresses: list[AuthorizationMap]
        :type bankaccounts: list[AuthorizationMap]
        :type emailaddresses: list[AuthorizationMap]
        :type facebook: bool
        :type github: bool
        :type grantedTo: str
        :type organizations: list[str]
        :type phonenumbers: list[AuthorizationMap]
        :type publicKeys: list[AuthorizationMap]
        :type username: str
        :rtype: Authorization
        """

        return Authorization(
            addresses=addresses,
            bankaccounts=bankaccounts,
            emailaddresses=emailaddresses,
            facebook=facebook,
            github=github,
            grantedTo=grantedTo,
            organizations=organizations,
            phonenumbers=phonenumbers,
            publicKeys=publicKeys,
            username=username,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'Authorization'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'addresses'
        val = data.get(property_name)
        if val is not None:
            datatypes = [AuthorizationMap]
            try:
                self.addresses = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'bankaccounts'
        val = data.get(property_name)
        if val is not None:
            datatypes = [AuthorizationMap]
            try:
                self.bankaccounts = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'emailaddresses'
        val = data.get(property_name)
        if val is not None:
            datatypes = [AuthorizationMap]
            try:
                self.emailaddresses = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'facebook'
        val = data.get(property_name)
        if val is not None:
            datatypes = [bool]
            try:
                self.facebook = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'github'
        val = data.get(property_name)
        if val is not None:
            datatypes = [bool]
            try:
                self.github = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'grantedTo'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.grantedTo = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'organizations'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.organizations = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'phonenumbers'
        val = data.get(property_name)
        if val is not None:
            datatypes = [AuthorizationMap]
            try:
                self.phonenumbers = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'publicKeys'
        val = data.get(property_name)
        if val is not None:
            datatypes = [AuthorizationMap]
            try:
                self.publicKeys = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'username'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.username = client_support.val_factory(val, datatypes)
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
