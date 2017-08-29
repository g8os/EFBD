"""
Auto-generated class for User
"""
from .Address import Address
from .BankAccount import BankAccount
from .DigitalAssetAddress import DigitalAssetAddress
from .EmailAddress import EmailAddress
from .FacebookAccount import FacebookAccount
from .GithubAccount import GithubAccount
from .Phonenumber import Phonenumber

from . import client_support


class User(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(addresses, bankaccounts, digitalwallet, emailaddresses, firstname, lastname, phonenumbers, publicKeys, username, expire=None, facebook=None, github=None):
        """
        :type addresses: list[Address]
        :type bankaccounts: list[BankAccount]
        :type digitalwallet: list[DigitalAssetAddress]
        :type emailaddresses: list[EmailAddress]
        :type expire: datetime
        :type facebook: FacebookAccount
        :type firstname: str
        :type github: GithubAccount
        :type lastname: str
        :type phonenumbers: list[Phonenumber]
        :type publicKeys: list[str]
        :type username: str
        :rtype: User
        """

        return User(
            addresses=addresses,
            bankaccounts=bankaccounts,
            digitalwallet=digitalwallet,
            emailaddresses=emailaddresses,
            expire=expire,
            facebook=facebook,
            firstname=firstname,
            github=github,
            lastname=lastname,
            phonenumbers=phonenumbers,
            publicKeys=publicKeys,
            username=username,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'User'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'addresses'
        val = data.get(property_name)
        if val is not None:
            datatypes = [Address]
            try:
                self.addresses = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'bankaccounts'
        val = data.get(property_name)
        if val is not None:
            datatypes = [BankAccount]
            try:
                self.bankaccounts = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'digitalwallet'
        val = data.get(property_name)
        if val is not None:
            datatypes = [DigitalAssetAddress]
            try:
                self.digitalwallet = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'emailaddresses'
        val = data.get(property_name)
        if val is not None:
            datatypes = [EmailAddress]
            try:
                self.emailaddresses = client_support.list_factory(val, datatypes)
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

        property_name = 'facebook'
        val = data.get(property_name)
        if val is not None:
            datatypes = [FacebookAccount]
            try:
                self.facebook = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'firstname'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.firstname = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'github'
        val = data.get(property_name)
        if val is not None:
            datatypes = [GithubAccount]
            try:
                self.github = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'lastname'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.lastname = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'phonenumbers'
        val = data.get(property_name)
        if val is not None:
            datatypes = [Phonenumber]
            try:
                self.phonenumbers = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'publicKeys'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.publicKeys = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

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
