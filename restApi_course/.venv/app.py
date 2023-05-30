from flask import Flask, request


app = Flask(__name__)


stores = [
    {
        "name": "MyStore",
        "items": [
            {"name": "Chair",
            "price": 15.99},
            {"name": "Table",
            "price": 34.99}
        ]
    }
]


# GET METHOD
@app.get("/get_store")
def get_store():
    return {"stores": stores}

# POST METHOD
@app.post("/create_store")
def create_store():
    request_data = request.get_json()
    new_store = {"name":request_data["name"], "items":request_data["items"]}
    stores.append(new_store)
    return new_store, 201

@app.post("/create_store/<string:name>/items")
def create_item(name):
    request_data = request.get_json()
    for store in stores:
        if store["name"] == name:
            new_item = {"name": request_data["name"] ,"price": request_data["price"]}
            if new_item not in store["items"]:
                store["items"].append(new_item)
                return new_item
            else:
                return {"msg":"Item already in store"}, 404
    return {"msg":"Store not found"}, 404


# GET SPECIFIC STORE and ITEM

@app.get("/store/<string:name>")
def get_specific_store(name):
    for store in stores:
        if store["name"] == name:
            return store
        else:
            return {"msg":"Store not found"}, 404

@app.get("/store/<string:name>/items")
def get_all_items_sin_specific_store(name):
    for store in stores:
        if store["name"] == name:
            return {"items": store["items"]}
        else:
            return {"msg":"Store not found"}, 404


app.run()
