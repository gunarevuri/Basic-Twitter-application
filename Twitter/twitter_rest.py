import pymongo
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo import ReturnDocument
import flask
import os
import json
import datetime
from bson import ObjectId

from flask import Flask, jsonify,request

"""Schema Version : 1

    User collection:
        "_id": username (str),
        schemaVersion: 1 (int),
        follows: [] (Array),
        date_created: Date 
        
    Posts collection:
        "_id":ObjectId(),
        "posted_by": username,
        schemaVersion: int,
        text: String()
        date_created: Date 
"""

"""Schema Version : 2

    User collection:
        "_id": username (str),
        schemaVersion: 1 (int),
        follows: [] (Array),
        date_created: Date,
        Num_posts: int
        
    Posts collection:
        "_id":ObjectId(),
        "posted_by": username,
        schemaVersion: int,
        text: String()
        date_created: Date 
"""

schemaVersion = 2
app = Flask(__name__)

@app.route('/home',methods=['GET'])
def home():
    return "hello"

def Check_user(user:str):
    try:
        client = pymongo.MongoClient(host='localhost',port=27017)
        db = client['Twitter_DB']
        col = db["Users"]
        return True if col.find_one({"_id":user}) else False
    except Exception as e:
        print(e)
        return False

@app.route('/users/', methods=['GET','POST'])
def get_user():
    global schemaVersion
    data = request.get_json()
    try:
        client = pymongo.MongoClient(host='localhost',port=27017,readConcernLevel='majority')
        db = client['Twitter_DB']
        col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        try:
            user = data.get('user')
            
            if not all([user, schemaVersion]):
                return jsonify({"Message":"Doesn't satisfy all the required fields"}),200
            elif Check_user(user):
                return jsonify({"Message":"User with {} username already exists".format(user)}),200
            
            doc = {"_id":user,"schemaVersion":schemaVersion,"date_created":datetime.datetime.now(),"follows":[]}
            res = col.insert_one(doc)
            print(res)
            print(f"inserted user {user}")
            response = {"Message":"Inserted user {}".format(str(user))}
            return jsonify(response),201
        except DuplicateKeyError:
            response = {"Message":"A user with username already exist"}
            return jsonify(response),200
        except Exception as e:
            print(e)
            return jsonify({"Message":"An error occured {}".format(str(e))})
        finally:
            client.close()
            print("connection closed")
    except Exception as e:
        print(e)
        return jsonify({"Message":"Error {}".format(str(e))})


@app.route('/users/<user>/profile',methods=['GET'])
def user_profile(user):
    global schemaVersion
    user = str(user)
    try:
        client = pymongo.MongoClient(host='0.0.0.0',port=27017)
        db = client.get_database('Twitter_DB')
        user_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        posts_col = db.get_collection(name='Posts',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        if not Check_user(user):
            return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
        try:
            user_obj = user_col.find_one({"_id":user})
            num_posts = user_obj.get('num_posts')
            if num_posts == None:
                num_posts = posts_col.count_documents({"posted_by":user})
                doc = user_col.find_one_and_update({"_id":user},{"$set":{"num_posts":num_posts,"schemaVersion":schemaVersion}},return_document=ReturnDocument.AFTER,upsert=True)
                return jsonify({"Profile":doc}),200
            else:
                return jsonify({"Profile":user_obj}),200
        except Exception as e:
            response = {"Message":"Error occured {}".format(str(e))}
            return jsonify(response),200
        finally:
            client.close()
            print("Connection closed")
    except Exception as e:
        print(e)
        return str(e)
    
@app.route("/users/<user>/followers", methods=['GET'])
def user_followers(user):
    user = str(user)
    try:
        client = pymongo.MongoClient(host='0.0.0.0',port=27017)
        db = client.get_database('Twitter_DB')
        user_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        if not Check_user(user):
            return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
        try:
            user_followers_list = list(user_col.find({"follows":user},{"_id":1}))
            return jsonify({"User":user,"followers":user_followers_list}),200
        except Exception as e:
            response = {"Message":"Error occured {}".format(str(e))}
            return jsonify(response),200
        finally:
            client.close()
            print("Connection closed")
    except Exception as e:
        print(e)
        return str(e)
    
@app.route('/users/<user>/follows', methods=['GET'])
def user_follows(user):
    user = str(user)
    try:
        client = pymongo.MongoClient(host='0.0.0.0',port=27017)
        db = client.get_database('Twitter_DB')
        user_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        if not Check_user(user):
            return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
        try:
            user_obj = user_col.find_one({"_id":user})
            return jsonify({"User":user,"follows":user_obj.get('follows',[])}),200
        except Exception as e:
            response = {"Message":"Error occured {}".format(str(e))}
            return jsonify(response),200
        finally:
            client.close()
            print("Connection closed")
    except Exception as e:
        print(e)
        return str(e)
    
@app.route('/users/<user>/followers/<follower>', methods=['GET','POST'])
def add_follower(user,follower):
    user , follower = str(user), str(follower)
    try:
        client = pymongo.MongoClient(host='0.0.0.0',port=27017)
        db = client.get_database('Twitter_DB')
        user_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
        if not Check_user(user):
            return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
        try:
            query = {"_id":follower}
            update = {"$addToSet":{"follows":user}}
            res = user_col.find_one_and_update(
                filter=query,
                update=update,
                return_document=ReturnDocument.AFTER
                )
            return jsonify({"Message":"Added {} to follows list of {}".format(follower,user)}),200
        except Exception as e:
            print(e)
            return jsonify({"Message":"Gor error {}".format(str(e))})
    except Exception as e:
        print(str(e))
        return jsonify({"Message":"Got Error {}".format(str(e))})




@app.route('/users/<user>/post', methods=['GET','POST'])
def user_post(user):
    user = str(user)
    global schemaVersion
    if request.method == 'POST':
        post = request.get_json()
        try:
            client = pymongo.MongoClient(host='0.0.0.0',port=27017)
            db = client.get_database('Twitter_DB')
            posts_col = db.get_collection(name='Posts',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            users_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            if not Check_user(user):
                return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
            text = post.get('text')
            posted_by = user
            if not any([text]):
                return jsonify({"Message":"Doesn't satisfy all the required fields"}),200
            try:
                posts_col.insert_one({"text":text,"posted_by":posted_by,"date_created":datetime.datetime.now(),"schemaVersion":schemaVersion})
                query = {"_id":user}
                users_col.find_one_and_update()
                return jsonify({"Message":"Post created"}),201
            except DuplicateKeyError:
                response = {"Message":"A Post with Same Id already exist"}
                return jsonify(response),200
            finally:
                client.close()
                print("Connection closed")
        except Exception as e:
            print(e)
            return str(e)
    if request.method == 'GET':
        try:
            client = pymongo.MongoClient(host='0.0.0.0',port=27017)
            db = client.get_database('Twitter_DB')
            posts_col = db.get_collection(name='Posts',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            query = {"posted_by":user}
            projec = {"_id":0}
            posts = list(posts_col.find(query,projection=projec))
            return jsonify({"Posts":posts}),200
        except Exception as e:
            print(e)
        finally:
            client.close()
            
            
@app.route('/users/<user>/posts/<post_id>', methods=["GET","DELETE"])
def user_delete_post(user,post_id):
    post_id = ObjectId(post_id)
    print(type(post_id))
    if request.method == 'GET':
        try:
            client = pymongo.MongoClient(host='0.0.0.0',port=27017)
            db = client.get_database('Twitter_DB')
            posts_col = db.get_collection(name='Posts',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            if not Check_user(user):
                return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
            post = posts_col.find_one({"_id":post_id})
            if post.get('_id'):
                post['_id'] = str(post['_id'])
                print(post)
                return jsonify({"Message":post}),200
        except Exception as e:
            print(str(e))
            return jsonify({"Message":"Got error {}".format(str(e))})
        finally:
            client.close()
            print("connection closed")
            
    if request.method == 'DELETE':
        try:
            client = pymongo.MongoClient(host='0.0.0.0',port=27017)
            db = client.get_database('Twitter_DB')
            posts_col = db.get_collection(name='Posts',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            users_col = db.get_collection(name='Users',write_concern=WriteConcern(w='majority'),read_concern=ReadConcern(level='majority'))
            if not Check_user(user):
                return jsonify({"Message":"User with name {} doesn't exists".format(str(user))}),200
            res = posts_col.find_one_and_delete({"_id":post_id})
            query = {"_id":user}
            update = {"$inc":{"num_posts":-1}}
            print("deleted")
            res2 = users_col.find_one_and_update(query, update,return_document=ReturnDocument.AFTER)
            return jsonify({"Message":"Post deleted","User Profile":res2}),200
        except Exception as e:
            print(e)
            return jsonify({"Message":"Got an error {}".format(str(e))})
        finally:
            client.close()
            print("Connection closed")
            
        
              
    
if __name__ == '__main__':
    app.run(debug=True)