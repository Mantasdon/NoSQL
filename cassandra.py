from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("banking")

"""print(session.execute("SELECT * FROM banking.Client").one())"""

"""
from_id = "2345"
# to_id =

###Adding to balance
balance = session.execute(
    "SELECT Balance FROM banking.Account WHERE AccountNumber = "2345" 
).one()
if balance:
    new_balance = balance.balance + 20

    session.execute(
        f"UPDATE banking.Account SET Balance = {new_balance} WHERE AccountNumber = {from_id}"
    )
    print("Added points to player")
else:
    print("Player not found.")

balance = session.execute(
    f"SELECT Balance FROM banking.Account WHERE AccountNumber = {from_id}"
).one()

print(balance.balance)

"""


def transfer_money(source_account_number, destination_account_number, amount):
    print("before transfer = ")

    # Read source account balance
    source_account_balance = session.execute(
        f"SELECT Balance FROM banking.Account WHERE AccountNumber = '{source_account_number}';"
    ).one()
    destination_account_balance = session.execute(
        f"SELECT Balance FROM banking.Account WHERE AccountNumber = '{destination_account_number}';"
    ).one()
    print(source_account_balance.balance)
    print(destination_account_balance.balance)

    # Check if the source account exists and has sufficient balance
    if source_account_balance and source_account_balance.balance >= amount:
        # Perform the transfer
        source_account_new_balance = source_account_balance.balance - amount

        session.execute(
            f"UPDATE Account SET Balance = {source_account_new_balance} WHERE AccountNumber = '{source_account_number}'"
        )

        destination_account_new_balance = destination_account_balance.balance + amount
        session.execute(
            f"UPDATE Account SET Balance = {destination_account_new_balance} WHERE AccountNumber = '{destination_account_number}'"
        )

        print(
            f"Money transferred successfully from {source_account_number} to {destination_account_number}."
        )
    else:
        print(
            "Transfer failed. Source account does not exist or has insufficient balance."
        )

    source_account_balance = session.execute(
        f"SELECT Balance FROM Account WHERE AccountNumber = '{source_account_number}'"
    ).one()
    destination_account_balance = session.execute(
        f"SELECT Balance FROM Account WHERE AccountNumber = '{destination_account_number}'"
    ).one()
    print(source_account_balance.balance)
    print(destination_account_balance.balance)


def all_acc_by_client():
    query = session.execute("SELECT * FROM banking.ClientAcc WHERE ClientID = 'Aaaa';")
    for eil in query:
        print(f"accountnumber: {eil.accountnumber}, ClientID: {eil.clientid}")


def all_acc_by_number():
    query = session.execute(
        "SELECT * FROM banking.AccByNumber WHERE AccountNumber = '5678';"
    )
    for eil in query:
        print(f"accountnumber: {eil.accountnumber}, ClientID: {eil.clientid}")


def acc_by_number():
    print(
        session.execute(
            "SELECT * FROM banking.Account WHERE AccountNumber = '2346'"
        ).one()
    )


def card_acc():
    print(
        session.execute(
            "SELECT * FROM banking.ClientAcc WHERE AccountNumber = '2346' AND ClientID = 'Aaaa' "
        ).one()
    )


def allclient():
    print("Clients")
    query = session.execute("SELECT * FROM banking.Client;")
    for eil in query:
        print(
            f"kliento_id: {eil.clientid}, vardas: {eil.firstname}, pavarde: {eil.lastname}, Ak: {eil.personalid}"
        )


def all_cards_by_card_number():
    query = session.execute(
        "SELECT * FROM banking.CreditCard WHERE CardNumber = '987444';"
    ).one()
    print(query)


def client_by_id():
    query = session.execute("SELECT * FROM banking.Client WHERE ClientID = 'Cccc';")
    for eil in query:
        print(
            f"clientid: {eil.clientid}, FirstName: {eil.firstname}, LastName: {eil.lastname}, PersonalID: {eil.personalid}"
        )


def client_by_perID():
    query = session.execute(
        "SELECT * FROM banking.ClientByPersonalID WHERE PersonalID = '11111';"
    )
    for eil in query:
        print(
            f"clientid: {eil.clientid}, FirstName: {eil.firstname}, LastName: {eil.lastname}, PersonalID: {eil.personalid}"
        )


def client_by_perID():
    query = session.execute(
        "SELECT * FROM banking.ClientByPersonalID WHERE PersonalID = '11111';"
    )
    for eil in query:
        print(
            f"clientid: {eil.clientid}, FirstName: {eil.firstname}, LastName: {eil.lastname}, PersonalID: {eil.personalid}"
        )


def acc_by_card():
    query = session.execute(
        "SELECT * FROM banking.AccByCard WHERE CardNumber = '000000';"
    )
    for eil in query:
        print(f"AccountNumber: {eil.accountnumber}")


# Example usage
source_account = "5678"
destination_account = "2345"
transfer_amount = 50.00


# all_client_cards()
# acc_by_number()
# all_client_acc()


card_acc()
#acc_by_card()#Account by card number
#client_by_perID() #client by personal id
#all_acc_by_number() #all accounts by account number
#all_cards_by_card_number()#all cards by card number
#client_by_id()#client by client id 
#all_acc_by_client()#all clients by client id

#transfer_money(source_account, destination_account, transfer_amount)


session.shutdown()
cluster.shutdown()
