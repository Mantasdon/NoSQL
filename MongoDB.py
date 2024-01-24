import pymongo


def delete_all_collection():
    # Delete all documents from the cafes collection
    cafes_collection.delete_many({})

    # Delete all documents from the orders collection
    orders_collection.delete_many({})

    # Delete all documents from the clients collection
    clients_collection.delete_many({})


"""def calculate_cafe_profits():
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$cafe_id",
                "total_profit": {"$sum": {"$items.price"}},
            }
        },
    ]

    result = list(orders_collection.aggregate(pipeline))

    # Print the results
    for item in result:
        cafe = cafes_collection.find_one({"_id": item["_id"]})
        print(f"Cafe Name: {cafe['name']}")
        print(f"Total Profits: ${item['total_profit']:.2f}")
        print("\n")
"""


def print_all_cafe():
    all_cafes = cafes_collection.find()
    it = 0
    for cafe in all_cafes:
        print("Cafe Name:", cafe["name"])
        print("Location:", cafe["location"])

        # Check if the "menu" field exists before accessing it
        if "menu" in cafe:
            print("Menu:")
            for item in cafe["menu"]:
                print(f"{item['item']}: ${item['price']:.2f}")
                it = it + 1
                print(it)

        else:
            print("Menu not available for this cafe.")

        print("\n")


def print_all_order_items():
    # Loop through and print each cafe document
    # Loop through and print items in all orders
    for order_id in order_ids:
        # Find the order document in the collection by its _id
        order = orders_collection.find_one({"_id": order_id})

        print("\nOrder ID:", order_id)

        # Check if the "items" field exists before accessing it
        if "items" in order:
            print("Order Items:")
            for item in order["items"]:
                print("Item:", item["item"])
                if "quantity" in item:
                    print("Quantity:", item["quantity"])
                if "price" in item:
                    print("Price: $", item["price"])
        else:
            print("No items in this order.")


def number_of_orders():
    pipeline = [{"$group": {"_id": "$cafe_id", "total_orders": {"$sum": 1}}}]

    result = list(orders_collection.aggregate(pipeline))

    # Print the results
    for item in result:
        cafe = cafes_collection.find_one({"_id": item["_id"]})
        print(f"Cafe Name: {cafe['name']}")
        print(f"Total Orders: {item['total_orders']}")
        print("\n")


def points_avg():
    pipeline = [
        {"$group": {"_id": None, "average_loyalty_points": {"$avg": "$loyalty_points"}}}
    ]

    result = list(clients_collection.aggregate(pipeline))
    # print(result)
    # Print the results
    for item in result:
        print(f"Average Loyalty Points of Clients: {item['average_loyalty_points']}")


# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["cafe_database"]

# Define the collections
cafes_collection = db["cafes"]
orders_collection = db["orders"]
clients_collection = db["clients"]

# Insert data into the cafe collection
cafes_data = [
    {
        "name": "Awesome Cafe",
        "location": "123 Main St",
        "menu": [
            {"item": "Espresso", "price": 3.5},
            {"item": "Latte", "price": 4.0},
            {"item": "Croissant", "price": 2.5},
        ],
    },
    {
        "name": "Cozy Corner Cafe",
        "location": "456 Elm St",
        "menu": [
            {"item": "Cappuccino", "price": 4.5},
            {"item": "Muffin", "price": 2.0},
        ],
    },
]

cafe_ids = [cafes_collection.insert_one(cafe).inserted_id for cafe in cafes_data]

# Insert data into the clients collection
clients_data = [
    {"name": "Lebron James", "email": "lebronjames@example.com", "loyalty_points": 200},
    {
        "name": "Cristiano Ronaldo",
        "email": "cristianoronaldo@example.com",
        "loyalty_points": 50,
    },
]

client_ids = [
    clients_collection.insert_one(client).inserted_id for client in clients_data
]

# Insert data into the orders collection with references to cafe and client
orders_data = [
    {
        "cafe_id": cafe_ids[0],
        "client_id": client_ids[0],
        "items": [
            {"item": "Espresso", "price": 2.0},
            {"item": "Latte", "price": 1.0},
        ],
    },
    {
        "cafe_id": cafe_ids[1],
        "client_id": client_ids[0],
        "items": [
            {"item": "Cappuccino", "price": 4.5},
            {"item": "Muffin", "price": 2.0},
        ],
    },
    {
        "cafe_id": cafe_ids[0],
        "client_id": client_ids[1],
        "items": [
            {"item": "Espresso", "price": 2.0},
            {"item": "Latte", "price": 1.0},
        ],
    },
    {
        "cafe_id": cafe_ids[1],
        "client_id": client_ids[1],
        "items": [
            {"item": "Cappuccino", "price": 4.5},
            {"item": "Muffin", "price": 2.0},
        ],
    },
]

order_ids = [orders_collection.insert_one(order).inserted_id for order in orders_data]


print_all_order_items()  # function used to get all embedded entities in order collection


number_of_orders()  # first aggregating query to get number of orders
points_avg()  # second aggregating query to get average points of customers in cafe
delete_all_collection()
