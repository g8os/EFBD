"""
Auto-generated class for Signature
"""

from . import client_support


class Signature(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(date, publicKey, signature, signedBy):
        """
        :type date: datetime
        :type publicKey: str
        :type signature: str
        :type signedBy: str
        :rtype: Signature
        """

        return Signature(
            date=date,
            publicKey=publicKey,
            signature=signature,
            signedBy=signedBy,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'Signature'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'date'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.date = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'publicKey'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.publicKey = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'signature'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.signature = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'signedBy'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.signedBy = client_support.val_factory(val, datatypes)
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
