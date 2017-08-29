"""
Auto-generated class for See
"""
from .SeeVersion import SeeVersion

from . import client_support


class See(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(globalid, uniqueid, username, versions):
        """
        :type globalid: str
        :type uniqueid: str
        :type username: str
        :type versions: list[SeeVersion]
        :rtype: See
        """

        return See(
            globalid=globalid,
            uniqueid=uniqueid,
            username=username,
            versions=versions,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'See'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'globalid'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.globalid = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'uniqueid'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.uniqueid = client_support.val_factory(val, datatypes)
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

        property_name = 'versions'
        val = data.get(property_name)
        if val is not None:
            datatypes = [SeeVersion]
            try:
                self.versions = client_support.list_factory(val, datatypes)
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
