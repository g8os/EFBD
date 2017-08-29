"""
Auto-generated class for JoinOrganizationInvitation
"""
from .EnumJoinOrganizationInvitationMethod import EnumJoinOrganizationInvitationMethod
from .EnumJoinOrganizationInvitationRole import EnumJoinOrganizationInvitationRole
from .EnumJoinOrganizationInvitationStatus import EnumJoinOrganizationInvitationStatus

from . import client_support


class JoinOrganizationInvitation(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(emailaddress, isorganization, method, organization, phonenumber, role, status, user, created=None):
        """
        :type created: datetime
        :type emailaddress: str
        :type isorganization: bool
        :type method: EnumJoinOrganizationInvitationMethod
        :type organization: str
        :type phonenumber: str
        :type role: EnumJoinOrganizationInvitationRole
        :type status: EnumJoinOrganizationInvitationStatus
        :type user: str
        :rtype: JoinOrganizationInvitation
        """

        return JoinOrganizationInvitation(
            created=created,
            emailaddress=emailaddress,
            isorganization=isorganization,
            method=method,
            organization=organization,
            phonenumber=phonenumber,
            role=role,
            status=status,
            user=user,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'JoinOrganizationInvitation'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'created'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.created = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'emailaddress'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.emailaddress = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'isorganization'
        val = data.get(property_name)
        if val is not None:
            datatypes = [bool]
            try:
                self.isorganization = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'method'
        val = data.get(property_name)
        if val is not None:
            datatypes = [EnumJoinOrganizationInvitationMethod]
            try:
                self.method = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'organization'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.organization = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'phonenumber'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.phonenumber = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'role'
        val = data.get(property_name)
        if val is not None:
            datatypes = [EnumJoinOrganizationInvitationRole]
            try:
                self.role = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'status'
        val = data.get(property_name)
        if val is not None:
            datatypes = [EnumJoinOrganizationInvitationStatus]
            try:
                self.status = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'user'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.user = client_support.val_factory(val, datatypes)
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
