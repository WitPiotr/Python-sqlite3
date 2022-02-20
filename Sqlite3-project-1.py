#File flightlegs.csv must be in the same directory as this program

# Import required libraries
try:
    import sqlite3
    import csv
    import pandas as pd
except:
    print("You can't import library.")


con = None

try:
    # Connecting to sqlite
    con = sqlite3.connect('task_2.db')

except sqlite3.Error as e:
    print(e)

#Creating new database if doesn't exist
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS FlightLeg (id INTEGER PRIMARY KEY AUTOINCREMENT, tailNumber TEXT, "
            "sourceAirportCode TEXT, destinationAirportCode TEXT, sourceCountryCode TEXT, destinationCountryCode "
            "TEXT, departureTimeUtc NUMERIC, landingTimeUtc NUMERIC)")



#Importing datas from csv to list
with open('flightlegs.csv', 'r', encoding='utf-8') as csvfile:
    csvreader = csv.DictReader(csvfile, delimiter=';')
    newDatabase = [(i['\ufefftailNumber'], i['source_airport_code'], i['source_country_code'], i['destination_airport_code'],
                    i['destination_country_code'], i['departure_time'], i['landing_time']) for i in csvreader]

#Checking database
cur.execute("SELECT * FROM FlightLeg")
tmp = cur.fetchall()

#Adding list to sql
try:
    if tmp[0][0] == "":
        cur.executemany("INSERT INTO FlightLeg (tailNumber, sourceAirportCode, sourceCountryCode, destinationAirportCode, "
                        "destinationCountryCode, departureTimeUtc, landingTimeUtc) VALUES ( ?, ?, ?, ?, ?, ? ,?);", newDatabase)
    else:
        None
except:
    cur.executemany(
        "INSERT INTO FlightLeg (tailNumber, sourceAirportCode, sourceCountryCode, destinationAirportCode, "
        "destinationCountryCode, departureTimeUtc, landingTimeUtc) VALUES ( ?, ?, ?, ?, ?, ? ,?);",
        newDatabase)

#Adding flightDuration and flightType columns
try:
    cur.execute("ALTER TABLE FlightLeg ADD COLUMN flightDuration NUMERIC")
    cur.execute("ALTER TABLE FlightLeg ADD COLUMN flightType TEXT")
except:
    None

#Adding duration of flights
cur.execute("SELECT (strftime('%s',landingTimeUtc) -  strftime('%s',departureTimeUtc))/60 FROM FlightLeg")
result = cur.fetchall()

j = 0
for i in result:
    j += 1
    cur.execute("UPDATE FlightLeg SET flightDuration = ? WHERE id = ?", (i[0], j))

#Selecting id where flight is domestic or international
cur.execute("SELECT id FROM FlightLeg WHERE sourceCountryCode != destinationCountryCode")
international = cur.fetchall()

cur.execute("SELECT id FROM FlightLeg WHERE sourceCountryCode == destinationCountryCode")
domestic = cur.fetchall()

#Adding to database division into domestic and international flights
for i in international:
    cur.execute("UPDATE FlightLeg SET flightType = ? WHERE id = ?", ("I", i[0]))
for i in domestic:
    cur.execute("UPDATE FlightLeg SET flightType = ? WHERE id = ?", ("D", i[0]))


print("Task 4.1")
cur.execute("SELECT tailNumber, COUNT(tailNumber)  FROM FlightLeg GROUP BY tailNumber ORDER BY COUNT(*) DESC LIMIT 3")
plane = cur.fetchall()
print(plane)
print("Plane " + plane[0][0] + " and "  + plane[1][0] + " have made the same number of flights.\n")


print("Task 4.2")
cur.execute("SELECT tailNumber, SUM(flightDuration) AS suma_minut FROM FlightLeg GROUP BY tailNumber ORDER BY "
            "suma_minut DESC LIMIT 1")
plane = cur.fetchall()
print(plane)

plane_name = plane[0][0]
minutes_of_flight= str(plane[0][1])

print(f"Plane {plane_name} flew the longest time: {minutes_of_flight}\n")

print("Task 4.3")
# minimum domestic
cur.execute("SELECT tailNumber, MIN(flightDuration)  AS minimum FROM FlightLeg WHERE flightType = ? "
            "GROUP BY flightDuration ORDER BY minimum ASC LIMIT 1", ("D",))
min_domestic = cur.fetchall()

# max domestic
cur.execute("SELECT tailNumber, MAX(flightDuration) AS minimum FROM FlightLeg WHERE flightType = ? "
            "GROUP BY flightDuration ORDER BY minimum DESC LIMIT 1", ("D",))
max_domestic = cur.fetchall()

# minimum international
cur.execute("SELECT tailNumber, MIN(flightDuration)  AS minimum FROM FlightLeg WHERE flightType = ? "
            "GROUP BY flightDuration ORDER BY minimum ASC LIMIT 1", ("I",))
min_international = cur.fetchall()

# max international
cur.execute("SELECT tailNumber, MAX(flightDuration) AS minimum FROM FlightLeg WHERE flightType = ? "
            "GROUP BY flightDuration ORDER BY minimum DESC LIMIT 1", ("I",))
max_international = cur.fetchall()


print(f"The shortest domestic flight is: {min_domestic[0][0]} lasted: {min_domestic[0][1]}\nThe longest domestic flight"
      f" is: {max_domestic[0][0]}lasted: {max_domestic[0][1]}\nThe shortest international flight is:"
      f" {min_international[0][0]} lasted: {min_international[0][1]}\nThe longest international flight is:"
      f" {max_international[0][0]} lasted: {max_international[0][1]}\n")


print("Task 4.4")
cur.execute("SELECT tailNumber, landingTimeUtc, departureTimeUtc FROM FlightLeg GROUP BY landingTimeUtc"
            " >= departureTimeUtc and landingTimeUtc <= departureTimeUtc")
fake_flights = cur.fetchall()
print(f"Wrong flights {fake_flights}\n")



#Saving database
con.commit()
#Closing connection with database
con.close()
