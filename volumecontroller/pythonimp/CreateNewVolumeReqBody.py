
from flask_wtf import Form
from wtforms.validators import DataRequired, Length, Regexp, NumberRange, required
from wtforms import TextField, FormField, IntegerField, FloatField, FileField, BooleanField, DateField, FieldList
from input_validators import multiple_of



class CreateNewVolumeReqBody(Form):
    
    blocksize = IntegerField(validators=[DataRequired(message="")])
    deduped = BooleanField(validators=[DataRequired(message="")])
    driver = TextField(validators=[])
    readOnly = BooleanField(validators=[])
    size = IntegerField(validators=[DataRequired(message="")])
    storagecluster = TextField(validators=[DataRequired(message="")])
    templatevolume = TextField(validators=[])
