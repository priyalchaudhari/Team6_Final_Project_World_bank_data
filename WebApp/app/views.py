from flask import render_template, flash, redirect
from app import app
from .forms import LoginForm


@app.route('/')
@app.route('/index')
def index():
    form = LoginForm()







    user = {'nickname': 'Miguel'}
    posts = [
        {
            'author': {'nickname': 'John'},
            'body': 'Beautiful day in Portland!'
        },
        {
            'author': {'nickname': 'Susan'},
            'body': 'The Avengers movie was so cool!'
        }
    ]
    return render_template('FirstPage.html',
                           title='Home',
                           form=form)


@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        flash('Login requested for OpenID="%s", remember_me=%s' %
              (form.openid.data, str(form.remember_me.data)))
        return redirect('/index')
    return render_template('login.html',
                           title='Sign In',
                           form=form,
                           providers=app.config['OPENID_PROVIDERS'])


@app.route('/submit', methods=['POST'])
def submit():
    form = LoginForm()
    print('============')
    print(form.country.data)
    print(form.indicator.data)
    print(form.startyear.data)
    print(form.nextyear.data)
    dataResponse = restCall(form.startyear.data,form.nextyear.data,form.country.data,form.indicator.data)
    finalData = splitArray(dataResponse)
    return render_template('Results.html',
                           output=finalData,
                           StateName=form.country.data,
                           Indicator=form.indicator.data)



    if form.validate_on_submit():
        print('Submitted')

def splitArray(dataResponse):
    count = len(dataResponse)
    newData = []
    newCount = int(count / 3)
    for i in range(0,newCount):
        tempCount = i * 3
        temp = []
        temp.append(dataResponse[tempCount])
        temp.append(dataResponse[tempCount+1])
        temp.append(dataResponse[tempCount+2])
        newData.append(temp)
    return newData

def restCall(StartYear,EndYear,country,Indicator):
    import urllib
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen
    # If you are using Python 3+, import urllib instead of urllib2

    import json

    data = {

        "Inputs": {

            "input1":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                },
            "input3":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                },
            "input2":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                },
            "input4":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                },
            "input5":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                },
            "input6":
                {
                    "ColumnNames": ["StartYear", "EndYear", "CountryName", "IndicatorCode"],
                    "Values": [[StartYear,EndYear,country,Indicator], ]
                }, },
        "GlobalParameters": {
        }
    }

    body = str.encode(json.dumps(data))

    if Indicator == 'BirthRate' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/1687b3bccc4e4c1bad27ab79d9d24d5b/execute?api-version=2.0&details=true'
        api_key = '0DtB7DlfsZb95H3v3XaHw+wLSfkDErZVH8qIVTcyNO1nTomlLBUVfBepon1hSNeE0jR+NYIS+j0tUPBhW+jSSQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
        print('BR')
    elif Indicator == 'Agriculture' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/d0faa24918d044b6a4bb3aa10cc587ad/execute?api-version=2.0&details=true'
        api_key = 'y9QRFZZXzj5ZkumEONmdITJ8VeHWUT18P5WqJuteSDZP2Bfaw9bc0eFXkd09A49vsuAbj8+/ZpYmbCfVSZZ+uw=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Age Dependency' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/d2751906b4904951afdab72bceed57f5/execute?api-version=2.0&details=true'
        api_key = 'Clt5IdIehIqmQbO/Ct5v76YuiUaNtNp7n0CRtqRr1/jSOtj1q4Pdn/za9JfmtZwE0zxUOlJ3PAAJ66VZRSGkgw=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'GDP growth' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/7d458276012648f6baa9fb2bb384b069/execute?api-version=2.0&details=true'
        api_key = 'nx/LIoat0jUW9WS+cxKFtuz2X+oJmZ7XHYW7TS4rkmL4HZAuUyMxkgo6zoOa+DBszftAHGy34xbPTbU+oViPIA=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Exports of goods and services' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/f7a0cd43357a4a4d97a9bcc769f752ae/execute?api-version=2.0&details=true'
        api_key = 'Hprs6PY/g4TfDnhrh58jYKGNb6/HMN+FI3VuIpib7QCi6gzLOEjzeEdAGOVsm2h4wJGo05dcZFabP8GwjD0EmQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Population growth' and country == 'AR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/28351a58589141afa44f8054e33a7f8b/execute?api-version=2.0&details=true'
        api_key = 'q729BvUQdlstNpghNvxPtchBZUDTrgq+ekRRmZs0eFTYIL7i7idu3rTv6HTeFl8kRkIBr9QGVi0MeF1hXBaAqA=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
		
    elif Indicator == 'BirthRate' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/8d5aa5fe2e5c4f099c3d0de17a27a9ba/execute?api-version=2.0&details=true'
        api_key = 'LsW27kW6GVdpB8bAQ2OmIWvSvNcmt0hzAeBeE7Via7ikA8Aw3YGMmPMCxYm+QWnUKJLaNjSLIJRA6vdioflo2w=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Agriculture' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/122cc058a3514bc69916a0e1bfc17c6e/execute?api-version=2.0&details=true'
        api_key = 'Vy2m1t7Hri6s51jDTlWmHPQtfnx8+hXlXT3Akyb5JIaI4eHc8z+bM0NFBpTHnejQ5RnXpbmMB+GTb2bSUimEVg=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Age Dependency' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/9615332b0cfc428987e429dd17b2d1d1/execute?api-version=2.0&details=true'
        api_key = 'vNDgentbzneNkxImAPtRf93BxOwBl3f9eFJu2UZx7oEuU222974RF97FlqLnK5S53H7yxJhT+15+x7nqaJC8DQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'GDP growth' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/4688b470d9bc494bbca49d7b971b79ff/execute?api-version=2.0&details=true'
        api_key = 'v/Ys/3xwUybX/iC5tp9zJqpkzaXdbawYkJ9SDXe4h2nefz5WZO0+oiozkQJnTik1QDAlCpU3iSPEJtLkIjGQBw=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Exports of goods and services' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/79d641596ad549a8aac2ae8090964bd2/execute?api-version=2.0&details=true'
        api_key = 'NCSzNdNjmuKGJo+P3SpcIUmUB5Hhay/hmP1botuEl5k1w5n39NJ9MniG8GSKzLMWLwwPCdtg8w0KcbBnpQveLg=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Population growth' and country == 'BR':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/14de0d9c4f1f49888d19631c55683e28/execute?api-version=2.0&details=true'
        api_key = 'oL8uYcPhl++irKv/+IPfgd+EME4rOSE6uRZnUKbaijFktFHHLNAJe4iFNiVX3tJgyHBAx4qdstap/n0yhOp0/w=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
		
    elif Indicator == 'BirthRate' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/51a615b6a1c5436d9d96680933fd149c/execute?api-version=2.0&details=true'
        api_key = '5wQfPfuLt9uudxQvjnLxhxXw6Y1Cl0P+t9UflqZrKOPz2v2f8x0W8mOjJhI+Ao+Nq34EcBYL1ngv2AIE47SOOA=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Agriculture' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/c88f2861f7e246b3898904a46fa20ace/execute?api-version=2.0&details=true'
        api_key = 'kNl6vRH2XEbwamZieKZjaijfqpu5WADBXNyStDvGu0R0T+BWxba8AA/Lw1lglbQ2MAlUQcEzderOnS0YA8P3lQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Age Dependency' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/026beae9b09c4422b3f48b1b5d560e5e/execute?api-version=2.0&details=true'
        api_key = 'vjC5eGlgxn5dAe/WjKZg1+ox56nv2vE9fUN26TLo8+zZWL2Ex6c50H65n9ekfX99v74gyEdc3EGnsZjNZn9y9Q=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'GDP growth' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/7d06020c01694adc9617818874077227/execute?api-version=2.0&details=true'
        api_key = '72jKiwXqdKm2JFBxpEYaHCT6EbgEXqxE9BupfKT0sgiASGfo59wBNBYxDuOBNq+9ERFiB0d4wwpbr9WteG8gdQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Exports of goods and services' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/cbb9ac13029542cab4f451456a404e1b/execute?api-version=2.0&details=true'
        api_key = 'n5lMEnzn/DLkGIe79raseJfXLtRp1DrACF3fX11/oCBGPprj54JHCadofMrzX0WYh1MVzzeooBUr3c+QYrf/WQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Population growth' and country == 'IN':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/c9b0ba3c6a3f474ab24429feb17a56f3/execute?api-version=2.0&details=true'
        api_key = 'AWsFPP+eIt5Zs5WO/8uHZNtTBc7eCBkU+2mA0mt2Nd9pF7GciL7T7yrsvqVH7OVDzSDR1IDeIn+AbXly0uyFtw=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
		
    elif Indicator == 'BirthRate' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/92530c4b3f5d47c3b304bfc45d53997f/execute?api-version=2.0&details=true'
        api_key = 'NFd/PGbRHvYkB3M7iIot+jYILkuk5QTlT+LYKOhdjEotTZQPQo/4KxkF29niCbkh1lhWgX+bvRRX8Sruj+14xw=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Agriculture' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/256f1162f9ac4650b44d061f33479c3d/execute?api-version=2.0&details=true'
        api_key = '+KzVM8xPlpG7vV+kof6vbYNYvZpq26+p0ZAvCwXrAxL3tjwBSO3Oewk+SrEDzhXyTdDHmy6HnomcLGvK+gfRiQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Age Dependency' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/bf08b1f975564e2eb46f39efdee93fad/execute?api-version=2.0&details=true'
        api_key = 'dZ3Lg5uXazWPLEIqeyED569y9YnCA99azdOmqQLxDRFrzMQQXpiyDKAA0FvC2mz4xB6JDBCff8IHYVH2Uc2E/A=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'GDP growth' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/be2f5cee4f3848f9babf11e5a3a0da96/execute?api-version=2.0&details=true'
        api_key = '7Dj02ijOh5zc/AtMF/UdtSslQ5TlRIcpXF32X6+KiV7FX6WYzq0g1dZ/m6YL5h3kp7Ndp6w/ybs0Ybl8S8a2wQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Exports of goods and services' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/21b4bffc469e4c1280e19714e1265f71/execute?api-version=2.0&details=true'
        api_key = 'I6MeM6X23xyTsqb2v8JKyiXHcVCAnxpdt4vvorCB1KPPivwBi3brvDT+ZOU/ySpvBRVNoSxS2nIXjGJ5RCcDBg=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}
    elif Indicator == 'Population growth' and country == 'EC':
        url = 'https://ussouthcentral.services.azureml.net/workspaces/1bfc9a8782274bcc86b83a6fbfd00352/services/3e91bea344244356a405a5b4b9e578f0/execute?api-version=2.0&details=true'
        api_key = 'RC8MMUJg6AR+wcuCOyDgL3reqMX4iYeHcREZhFrvqK1/vMi9CGI9vIljmhfUlPMTwneSSvG9kw5cfcgR/lTGyQ=='  # Replace this with the API key for the web service
        headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}

    req = Request(url, body, headers)

    try:
        response = urlopen(req)

        # If you are using Python 3+, replace urllib2 with urllib.request in the above code:
        # req = urllib.request.Request(url, body, headers)
        # response = urllib.request.urlopen(req)

        # result = response.readall().decode('utf-8')
        result = response.read().decode('utf-8')
        print(type(result))
        print(result)
        import codecs

        # reader = codecs.getreader("utf-8")
        # obj = json.load(reader(response))
        final_result = json.loads(result)
        res = final_result['Results']['output1']['value']['Values'][0];
        print(res)
        return res
    except HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())

        print(json.loads(error.read().decode('utf-8')))