"""
Auto-generated class for GithubAccount
"""

from . import client_support


class GithubAccount(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(avatar_url, html_url, id, login, name):
        """
        :type avatar_url: str
        :type html_url: str
        :type id: int
        :type login: str
        :type name: str
        :rtype: GithubAccount
        """

        return GithubAccount(
            avatar_url=avatar_url,
            html_url=html_url,
            id=id,
            login=login,
            name=name,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'GithubAccount'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'avatar_url'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.avatar_url = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'html_url'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.html_url = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'id'
        val = data.get(property_name)
        if val is not None:
            datatypes = [int]
            try:
                self.id = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'login'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.login = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'name'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.name = client_support.val_factory(val, datatypes)
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
