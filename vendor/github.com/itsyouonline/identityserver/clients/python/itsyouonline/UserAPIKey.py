"""
Auto-generated class for UserAPIKey
"""
from .Label import Label

from . import client_support


class UserAPIKey(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(apikey, applicationid, label, scopes, username):
        """
        :type apikey: str
        :type applicationid: str
        :type label: Label
        :type scopes: list[str]
        :type username: str
        :rtype: UserAPIKey
        """

        return UserAPIKey(
            apikey=apikey,
            applicationid=applicationid,
            label=label,
            scopes=scopes,
            username=username,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'UserAPIKey'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'apikey'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.apikey = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'applicationid'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.applicationid = client_support.val_factory(val, datatypes)
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

        property_name = 'scopes'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.scopes = client_support.list_factory(val, datatypes)
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
