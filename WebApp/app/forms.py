from flask_wtf import Form
from wtforms import StringField, BooleanField, SelectField
from wtforms.validators import DataRequired


class LoginForm(Form):
    # openid = StringField('openid', validators=[DataRequired()])
    country = SelectField('country', choices=[('IN', 'India'),('AR', 'Argentina'),('BR','Brazil'),
                                          ('EC','Ecuador')])
    indicator = SelectField('indicator', choices=[('BirthRate','BirthRate'),('Agriculture','Agriculture'),
                                                  ('Age Dependency','Age Dependency'),
												  ('GDP growth','GDP growth'), 
												  ('Exports of goods and services','Exports of goods and services'),
												  ('Population growth','Population growth')])
    newArray = []
    for i in range(1960,2025):
        newArray.append((i,i))
    startyear = SelectField('startyear', choices=newArray)
    nextyear = SelectField('nextyear', choices=newArray)    
# remember_me = BooleanField('remember_me', default=False)