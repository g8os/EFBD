"""
Auto-generated class for GetNotificationsReqBody
"""
from .ContractSigningRequest import ContractSigningRequest
from .JoinOrganizationInvitation import JoinOrganizationInvitation
from .MissingScopes import MissingScopes

from . import client_support


class GetNotificationsReqBody(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(approvals, contractRequests, invitations, missingscopes):
        """
        :type approvals: list[JoinOrganizationInvitation]
        :type contractRequests: list[ContractSigningRequest]
        :type invitations: list[JoinOrganizationInvitation]
        :type missingscopes: list[MissingScopes]
        :rtype: GetNotificationsReqBody
        """

        return GetNotificationsReqBody(
            approvals=approvals,
            contractRequests=contractRequests,
            invitations=invitations,
            missingscopes=missingscopes,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'GetNotificationsReqBody'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'approvals'
        val = data.get(property_name)
        if val is not None:
            datatypes = [JoinOrganizationInvitation]
            try:
                self.approvals = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'contractRequests'
        val = data.get(property_name)
        if val is not None:
            datatypes = [ContractSigningRequest]
            try:
                self.contractRequests = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'invitations'
        val = data.get(property_name)
        if val is not None:
            datatypes = [JoinOrganizationInvitation]
            try:
                self.invitations = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'missingscopes'
        val = data.get(property_name)
        if val is not None:
            datatypes = [MissingScopes]
            try:
                self.missingscopes = client_support.list_factory(val, datatypes)
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
