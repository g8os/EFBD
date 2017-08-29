"""
Auto-generated class for Organization
"""
from .RequiredScope import RequiredScope

from . import client_support


class Organization(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(dns, globalid, includes, includesuborgsof, members, orgmembers, orgowners, owners, publicKeys, requiredscopes):
        """
        :type dns: list[str]
        :type globalid: str
        :type includes: list[str]
        :type includesuborgsof: list[str]
        :type members: list[str]
        :type orgmembers: list[str]
        :type orgowners: list[str]
        :type owners: list[str]
        :type publicKeys: list[str]
        :type requiredscopes: list[RequiredScope]
        :rtype: Organization
        """

        return Organization(
            dns=dns,
            globalid=globalid,
            includes=includes,
            includesuborgsof=includesuborgsof,
            members=members,
            orgmembers=orgmembers,
            orgowners=orgowners,
            owners=owners,
            publicKeys=publicKeys,
            requiredscopes=requiredscopes,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'Organization'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'dns'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.dns = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

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

        property_name = 'includes'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.includes = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'includesuborgsof'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.includesuborgsof = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'members'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.members = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'orgmembers'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.orgmembers = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'orgowners'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.orgowners = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'owners'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.owners = client_support.list_factory(val, datatypes)
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

        property_name = 'requiredscopes'
        val = data.get(property_name)
        if val is not None:
            datatypes = [RequiredScope]
            try:
                self.requiredscopes = client_support.list_factory(val, datatypes)
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
