"""
Auto-generated class for GetUserOrganizationsReqBody
"""

from . import client_support


class GetUserOrganizationsReqBody(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(member, owner):
        """
        :type member: list[str]
        :type owner: list[str]
        :rtype: GetUserOrganizationsReqBody
        """

        return GetUserOrganizationsReqBody(
            member=member,
            owner=owner,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'GetUserOrganizationsReqBody'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'member'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.member = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'owner'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.owner = client_support.list_factory(val, datatypes)
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
