from flask import Flask, jsonify
app = Flask(__name__)

from flask import Response
from flask import request, send_file
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import simplejson as json
from decimal import Decimal
from flask_cors import CORS, cross_origin
from boto3.dynamodb.conditions import Key
from contextlib import closing
import os
from tempfile import gettempdir

tableName = "Films-"
translate = boto3.client(service_name='translate', region_name='us-east-1', use_ssl=True)
polly = boto3.client(service_name='polly', region_name='us-east-1', use_ssl=True)
# Flip this boolean to switch between testing or deployment mode
testing = False
groupId = 'groupId'
groupIdFree = 'free'
groupIdPaid = 'paid'


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

@app.route('/key', methods=['GET'])
@cross_origin()
def getKey():
    curGroupId = request.args.get(groupId)
    if curGroupId is not None:
        if curGroupId == groupIdPaid:
            return Response("MMvlWv4gPh9kniWJfjMPJ5T5h0s0ep1D2CLjqDSK", status=200, mimetype='text/plain')
        if curGroupId == groupIdFree:
            return Response("4TIovpYOER55GGsAw7lyX9c7su4qmsMH3n1AyZm1", status=200, mimetype='text/plain')
    invalidGroupIdText = json.dumps({"message" : "Invalid groupId, no x-api-key retrieved."})
    return Response(invalidGroupIdText, status=200, mimetype='application/json')


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
    curGroupId = request.args.get(groupId)
    
    if curGroupId is not None and (curGroupId==groupIdFree or curGroupId==groupIdPaid):
        table = dynamodb.Table(tableName + curGroupId)  
        
        year = request.args.get('year')
        movie = request.args.get('movie')
        
        # If year is not None, get all movies
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
    invalidGroupIdText = json.dumps({"message" : "Invalid groupId, no table retrieved."})
    return Response(invalidGroupIdText, status=200, mimetype='application/json')
    

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
    

@app.route('/translate')
@cross_origin()
def translation():
    curGroupId = request.args.get(groupId)
    
    if curGroupId is not None and (curGroupId==groupIdFree or curGroupId==groupIdPaid):
        table = dynamodb.Table(tableName + curGroupId)
        
        movies = getMovies(table)
        moviesJson = json.loads(movies)
        #print(movies)
        #print(moviesJson)
        
        for cMovieJSON in moviesJson:
            #print(cMovieJSON)
            for key in cMovieJSON:
                #print("%s | %s" %(key, cMovieJSON[key])
                value = cMovieJSON[key]
                translatedResult = translate.translate_text(Text=str(value), SourceLanguageCode="en", TargetLanguageCode="zh")
                translatedText = translatedResult.get('TranslatedText')
                #print('TranslatedText: ' + translatedText)
                cMovieJSON[key] = translatedText
        #print(moviesJson)

        jsonStr = json.dumps(moviesJson, ensure_ascii=False).encode('utf8')
        #print(jsonStr)
        #print(json.loads(jsonStr))
    
        return Response(jsonStr, status=200, mimetype='application/json')
    invalidGroupIdText = json.dumps({"message" : "Invalid groupId, no table translation retrieved."})
    return Response(invalidGroupIdText, status=200, mimetype='application/json')

@app.route('/polly')
@cross_origin()
def runPolly():
    curGroupId = request.args.get(groupId)
    
    if curGroupId is not None and (curGroupId==groupIdFree or curGroupId==groupIdPaid):
        table = dynamodb.Table(tableName + curGroupId)
        
        movies = getMovies(table)
        try:
            print(os.getcwd())
            print(os.listdir())
            response = polly.synthesize_speech(Text=movies, OutputFormat="mp3", VoiceId="Joanna")
            with open('speech.mp3', 'wb') as f:
                f.write(response['AudioStream'].read())
                f.close()
            try:
                return send_file("speech.mp3", as_attachment=True)
            except Exception as e:
                print(e)
            #file = open('speech.mp3', 'wb')
            #file.write(response['AudioStream'].read())
            #file.close()
        except(BotoCoreError, ClientError) as error:
            print(error)       
        return Response(json.dumps({'message' : 'Error: Did not synthesize speech.'}), status=200, mimetype='application/json')
    invalidGroupIdText = json.dumps({"message" : "Invalid groupId, no table speech retrieved."})
    return Response(invalidGroupIdText, status=200, mimetype='application/json')


@app.route('/getTable')
@cross_origin()
def getEntry():
    table = dynamodb.Table(tableName)
    js = json.dumps({'name' : 'get entry'}, indent=4)

    try:
        response = table.get_item(Key={'year': 2020, 'title': 'Haha the movie'})
    except ClientError as e:
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
def putEntry(title, year):
    js = json.dumps({'message' : 'Put movie error.'})
    try:
        table = dynamodb.Table(tableName)
        js = table.put_item(
        Item={
                'year': year,
                'title': title
            }
        )
    except:
        js = json.dumps({'name' : 'put entry already exists'})

    return Response(js, status=200, mimetype='application/json')


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