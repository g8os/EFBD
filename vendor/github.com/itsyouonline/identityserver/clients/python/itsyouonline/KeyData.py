"""
Auto-generated class for KeyData
"""

from . import client_support


class KeyData(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(algorithm, comment=None, timestamp=None):
        """
        :type algorithm: str
        :type comment: str
        :type timestamp: datetime
        :rtype: KeyData
        """

        return KeyData(
            algorithm=algorithm,
            comment=comment,
            timestamp=timestamp,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'KeyData'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'algorithm'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.algorithm = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'comment'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.comment = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'timestamp'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.timestamp = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
