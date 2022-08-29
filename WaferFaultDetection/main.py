from ast import Not
from crypt import methods
import os
from flask import Flask,request,render_template,Response
from flask_cors import CORS,cross_origin
from wsgiref import simple_server
import flask_monitoringdashboard


os.putenv('LANG','en_US.UTF-8')
os.putenv('LC_ALL','en_US.UTF-8')

app = Flask(__name__)
flask_monitoringdashboard.bind(app)
CORS(app)


@app.route('/',methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route('/train',methods=['POST'])
@cross_origin
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path=request.json['folderPath']
            
    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")



