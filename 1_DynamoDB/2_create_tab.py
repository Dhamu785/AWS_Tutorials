import boto3
from pprint import pprint

client = boto3.client('dynamodb')

def create_tab():
    table = client.create_table(
        TableName = "Movies",
        KeySchema = [
            {"AttributeName": "year", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"}
        ],
        AttributeDefinitions = [
            {"AttributeName": "year", "AttributeType": "S"},
            {"AttributeName": "title", "AttributeType": "S"}
        ],
        ProvisionedThroughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    
    return table

def add_items(name,yr,rating,cast):
    response = client.put_item(
        TableName = "Movies",
        Item = {
            "year": {"S": "{}".format(yr)},
            "title": {"S": name},
            "rating": {"S": rating},
            "cast": {"S": cast}
        }
    )
    
    return response

def get_items(year):
    try:
        response = client.get_item(
            TableName = "Movies",
            Key = {'year':{'S':"{}".format('20017')},'title':{'S': "The Big New Movie"}}
        )
    except Exception as e:
        print(e)
    else:
        return response

def update_item():
    row = client.update_item(
        TableName = "Movies",
        Key = {'year':{'S':"{}".format('A.D. 2017')},'title':{'S': "The Big New Movie"}},
        ExpressionAttributeNames = {"#ct": "cast", "#r": "rating", "#n":"new2"},
        ExpressionAttributeValues = {":ct": {"S": "Kali"}, ":r": {"S": "11"}, ":n": {"S": "new"}},
        UpdateExpression = "SET #ct = :ct, #r = :r, #n = :n",
        ReturnValues = "UPDATED_NEW"
    )
    return row

def make_increment():
    tab = client.update_item(
        TableName = "Movies",
        Key = {'year':{'S':"{}".format('2000')},'title':{'S': "chandramuki"}},
        ExpressionAttributeNames = {"#b": "Budget"},
        # ExpressionAttributeValues = {":b": {"N": "10"}},
        # ExpressionAttributeValues = {":b": {"N": "%i"%(10)}},
        # ExpressionAttributeValues = {":b": {"N": "{}".format(10)}},
        ExpressionAttributeValues = {":b": {"N": f"{10}"}},
        
        
        UpdateExpression = "SET #b = #b + :b",
        ReturnValues = "UPDATED_NEW"
    )
    
    return tab

def delete_item():
    try:
        tab = client.delete_item(
            TableName = "Movies",
            Key = {'year':{'S':"{}".format('2000')},'title':{'S': "chandramuki"}},
            ExpressionAttributeValues = {":b": {"N": f"{100}"}},
            ConditionExpression = "Budget <= :b"
        )
    except Exception as e:
        if e == "ConditionalCheckFailedException":
            print("BAD")
    else:
        return tab

if __name__ == "__main__":
    
    # Create table in the dnamoDB-AWS
    # tab = create_tab()
    # print(tab)
    
    # Add items in the created table
    # res = add_items("The Big New Movie", "2018", "1.5", "Larry, Moe, Curly")
    # print(res)
    
    # Get items from the created table
    # res = get_items("2018")
    # pprint(f'The returned data are: {res}')
    
    # Update an tem in the table
    # response = update_item()
    # pprint(response)
    
    # Data increment in the table
    # res = make_increment()
    # pprint(res)
    
    # Delete data
    res = delete_item()
    pprint(res)