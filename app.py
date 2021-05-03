from flask import Flask, jsonify
app = Flask(__name__)

from flask import Response
from flask import request
import boto3
from botocore.exceptions import ClientError
import simplejson as json
from decimal import Decimal
from flask_cors import CORS, cross_origin
from boto3.dynamodb.conditions import Key

tableName = "Films"
# Flip this boolean to switch between testing or deployment mode
testing = False

dynamodb = None
if testing:
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:80")
else:
    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    client = boto3.client('dynamodb', region_name='us-east-1')
    
def getItemMap(data):
    infoData = data['info']
    if infoData is not None:
        postData = {'year' : str(data['year']), 'rating' : str(infoData['rating']), 'plot' : str(infoData['plot']), 'title' : str(data['title'])}
    else:
        postData = {'year' : str(data['year']), 'title' : star(data['title'])}
    return postData
    
def getItemJSON(data):
    postData = getItemMap(data)
    return json.dumps(postData, indent=4)
    

@app.route('/')
@cross_origin()
def home():
    welcomeStr = "Welcome to the home page."
    return Response(welcomeStr, status=200, mimetype='text/plain')


# To test:
# curl "http://0.0.0.0:80/find?year=2020&movie=Haha%20the%20movie"
@app.route('/find', methods=['GET'])
@cross_origin()
def find():
    findJSON = "{ \"msg\": \"Please enter a year and movie query value.\"}"
    year = request.args.get('year')
    movie = request.args.get('movie') 
    
    if year is not None and movie is not None:
        try:
            year = int(year)
            table = dynamodb.Table(tableName)

            response = table.query(
                KeyConditionExpression=Key('year').eq(year)
            )
        
            retList = []
            for cItem in response['Items']:
                if movie in cItem['title']:
                    #cJSON = getItemMap(cItem)
                    #retList.append(cJSON)
                    print(cItem)
                    retList.append(cItem)
        
            retMap = {'FindData' : retList}
            findJSON = json.dumps(retMap, indent=4)
            return Response(findJSON, status=200, mimetype='application/json')
        except Exception as e:
            print(e)
        
    return Response(findJSON, status=200, mimetype='application/json')

@app.route('/table')
@cross_origin()
def getTable():
    table = dynamodb.Table(tableName)
    year = request.args.get('year')
    movie = request.args.get('movie')
    
    if year is not None:
        if movie is not None:

            response = table.query(
                ProjectionExpression="#yr, title, info.genres, info.actors[0]",
                ExpressionAttributeNames={"#yr": "year"},
                KeyConditionExpression=Key('year').eq(year) & Key('title').begins_with(movie)
            )
            return Response(json.dumps(response['Items'], use_decimal=True), status=200, mimetype='application/json')
        else:
            year = int(year)
            response = table.query(
                KeyConditionExpression=Key('year').eq(year)
            )
            return Response(json.dumps(response['Items'], use_decimal=True), status=200, mimetype='application/json')
    else:
        return Response(getMovies(table), status=200, mimetype='application/json')

# Returns all the movies, essentially (1900-2025 range)
def getMovies(table):
    year_range = (1900, 2025)
    scan_kwargs = {
        'FilterExpression': Key('year').between(*year_range),
        'ProjectionExpression': "#yr, title, info.rating",
        'ExpressionAttributeNames': {"#yr": "year"}
    }
    
    done = False
    startKey = None
    js = ""
    while not done:
        if startKey:
            scan_kwargs['ExclusiveStartKey'] = startKey
        response = table.scan(**scan_kwargs)
        js = json.dumps(response.get('Items'))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return js
    

@app.route('/getTable')
@cross_origin()
def getEntry():
    table = dynamodb.Table(tableName)
    js = json.dumps({'name' : 'get entry'}, indent=4)

    try:
        response = table.get_item(Key={'year': 2020, 'title': 'Haha the movie'})
    except ClientError as e:
        print("hi")
        js = json.dumps({'name' : e.response['Error']['Message']}, indent=4)
    else:
        try:
            data = response['Item']
            js = getItemJSON(data)
        except Exception as e:
            print(e)
            js = json.dumps({'name' : 'entry not found'}, indent=4)

    resp = Response(js, status=200, mimetype='application/json')
    return resp

@app.route('/putEntry')
@cross_origin()
def putEntry():
    js = json.dumps({'name' : 'put entry'})
    try:
        table = dynamodb.Table(tableName)
        response = table.put_item(
        Item={
                'year': 2020,
                'title': 'Haha the movie',
                'info': {
                    'plot': 'No plot',
                    'rating': 5
                }
            }
        )
    except:
        js = json.dumps({'name' : 'put entry already exists'})

    resp = Response(js, status=200, mimetype='application/json')
    return resp


@app.route('/clearEntry')
@cross_origin()
def clearEntry():
    js = json.dumps({'msg' : 'No entries cleared. Put year={var}&movie={var} in the query.'})

    year = request.args.get('year')
    movie = request.args.get('movie')
    
    if year is not None and movie is not None:
        try:
            year = int(year)
            table = dynamodb.Table(tableName)
            response = table.delete_item(
                Key={
                    'year': year,
                    'title': movie
                },
            )
            js = json.dumps(response)
        except Exception as e:
            print(e)

    resp = Response(js, status=200, mimetype='application/json')
    return resp

@app.route('/newTable')
@cross_origin()
def newTable():
    js = json.dumps({'name' : 'new table'})
    try:
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=[
                {
                    'AttributeName': 'title',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'year',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },
                                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            })
    except:
        js = json.dumps({'name' : 'new table already exists'})     

    resp = Response(js, status=200, mimetype='application/json')
    return resp


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
