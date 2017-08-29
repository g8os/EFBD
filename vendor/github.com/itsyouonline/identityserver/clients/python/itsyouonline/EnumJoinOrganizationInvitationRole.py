from enum import Enum


class EnumJoinOrganizationInvitationRole(Enum):
    owner = "owner"
    member = "member"
    orgowner = "orgowner"
    orgmember = "orgmember"
