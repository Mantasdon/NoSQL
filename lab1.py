import redis

# Connect to the Redis server
r = redis.Redis(host="localhost", port=6379, db=0)
p = r.pipeline()


def check_seat_availability(session_id):
    # Retrieve the list of booked seats for the given session
    booked_seats = r.smembers(session_id)

    decoded_seats = [seat.decode("utf-8") for seat in booked_seats]
    return decoded_seats


def book_seat(session_id, seat_identifier):
    return p.sadd(session_id, seat_identifier) == 1

    # Check if the seat is already booked
    if(p.sadd(session_id, seat_identifier) == 1)
        return True
    else:
        return False
    
    '''p.watch(session_id)# checks that value doesnt change
    if r.sismember(session_id, seat_identifier):
        return False  # Seat is already booked
    else:
        # Book the seat
        
        p.multi()
       
        p.execute()

        return True  # Seat booked successfully'''


def display_available_seats(session_id):
    # Create a list of all possible seat identifiers from A1 to I10
    seat_identifiers = []

    for row in range(9):  # Rows A to I
        for column in range(1, 11):  # Columns 1 to 10
            seat = chr(65 + row) + str(column)  # Convert row to letter (A to I)
            seat_identifiers.append(seat)

    # Get the list of booked seats for the session
    booked_seats = check_seat_availability(session_id)

    # Calculate available seats
    available_seats = []

    for seat in seat_identifiers:
        if seat not in booked_seats:
            available_seats.append(seat)

    return available_seats


def Create_session():  # making session id
    session_input = input(
        "Choose a movie: Barbie = 1, Oppenheimer = 2, Spiderman = 3, Batman = 4\n"
    )
    session = ""

    session = session + session_input  # getting first number for id

    session_input = input(
        "Choose a month: January = 1, February = 2, March = 3, April = 4, May = 5, June = 6, July = 7, October = 8, September = 9, October = 10, November = 11, December = 12\n"
    )
    session = session + session_input  # getting second number for id
    session_input = input("Choose a day: 1-30\n")
    session = session + session_input  # getting completed id

    return session


session_id = Create_session()
print("SESSION ID = ")
print(session_id)


while True:
    print("Session:" + str(session_id))
    print("1. Check Seat Availability")
    print("2. Book a Seat")
    print("3. Exit")

    choice = input("Enter your choice: ")  # getting input

    if choice == "1":
        available_seats = display_available_seats(session_id)
        print("Available Seats:", available_seats)
    elif choice == "2":
        seat_identifier = input(
            "Enter seat identifier to book (e.g., A1, D10): "
        )  # getting seat number
        if book_seat(session_id, seat_identifier):  # checks is the seat free
            print("Seat " + str(seat_identifier) + " booked successfully!")

        else:
            print("Seat " + str(seat_identifier) + " is already booked.")
    elif choice == "3":
        break
    else:
        print("Invalid choice. Please try again.")
