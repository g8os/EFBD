"""
Auto-generated class for SeeView
"""

from . import client_support


class SeeView(object):
    """
    auto-generated. don't touch.
    """

    @staticmethod
    def create(category, content_type, creation_date, end_date, globalid, keystore_label, link, markdown_full_description, markdown_short_description, signature, start_date, uniqueid, username, version):
        """
        :type category: str
        :type content_type: str
        :type creation_date: str
        :type end_date: str
        :type globalid: str
        :type keystore_label: str
        :type link: str
        :type markdown_full_description: str
        :type markdown_short_description: str
        :type signature: str
        :type start_date: str
        :type uniqueid: str
        :type username: str
        :type version: int
        :rtype: SeeView
        """

        return SeeView(
            category=category,
            content_type=content_type,
            creation_date=creation_date,
            end_date=end_date,
            globalid=globalid,
            keystore_label=keystore_label,
            link=link,
            markdown_full_description=markdown_full_description,
            markdown_short_description=markdown_short_description,
            signature=signature,
            start_date=start_date,
            uniqueid=uniqueid,
            username=username,
            version=version,
        )

    def __init__(self, json=None, **kwargs):
        if json is None and not kwargs:
            raise ValueError('No data or kwargs present')

        class_name = 'SeeView'
        create_error = '{cls}: unable to create {prop} from value: {val}: {err}'
        required_error = '{cls}: missing required property {prop}'

        data = json or kwargs

        property_name = 'category'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.category = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'content_type'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.content_type = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'creation_date'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.creation_date = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'end_date'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.end_date = client_support.val_factory(val, datatypes)
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

        property_name = 'keystore_label'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.keystore_label = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'link'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.link = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'markdown_full_description'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.markdown_full_description = client_support.val_factory(val, datatypes)
            except ValueError as err:
                raise ValueError(create_error.format(cls=class_name, prop=property_name, val=val, err=err))
        else:
            raise ValueError(required_error.format(cls=class_name, prop=property_name))

        property_name = 'markdown_short_description'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.markdown_short_description = client_support.val_factory(val, datatypes)
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

        property_name = 'start_date'
        val = data.get(property_name)
        if val is not None:
            datatypes = [str]
            try:
                self.start_date = client_support.val_factory(val, datatypes)
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

        property_name = 'version'
        val = data.get(property_name)
        if val is not None:
            datatypes = [int]
            try:
                self.version = client_support.val_factory(val, datatypes)
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
