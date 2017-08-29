"""
Auto-generated class for companyview
"""

from . import client_support


class companyview(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(globalid, publicKeys, expire=None, info=None, organizations=None, taxnr=None):
        """
        :type expire: datetime
        :type globalid: str
        :type info: list[str]
        :type organizations: list[str]
        :type publicKeys: list[str]
        :type taxnr: str
        :rtype: companyview
        """

        return companyview(
            expire=expire,
            globalid=globalid,
            info=info,
            organizations=organizations,
            publicKeys=publicKeys,
            taxnr=taxnr,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'companyview'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'expire'
        val = data.get(property_name)
        if val is not None:
            datatypes = [datetime]
            try:
                self.expire = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

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

        property_name = 'info'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.info = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

        property_name = 'organizations'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.organizations = client_support.list_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

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

        property_name = 'taxnr'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.taxnr = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))

    def __str__(self):
        return self.as_json(indent=4)

    def as_json(self, indent=0):
        return client_support.to_json(self, indent=indent)

    def as_dict(self):
        return client_support.to_dict(self)
