import sqlite3
import time

connection = sqlite3.connect("database.db")
cur = connection.cursor()


def answer(i):
    global questions
    print("Question " + str(i) + " : ")
    res = cur.execute(questions[i-1])
    #Eventually print research
    """for row in res:
        for elt in range (len(row)):
            print(row[elt])"""
            

def add_index(i):
    global connection

    #INDEX FOR QUESTION 1
    if(i == 1):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_primaryName ON Persons(primaryName)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pid ON Principals(pid)")
    #INDEX FOR QUESTION 2
    elif(i == 2):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_genre ON Genres(genre)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mid ON Ratings(mid)")
    #INDEX FOR QUESTION 3
    elif(i == 3):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_region ON Titles(region)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pid ON Writers(pid)")
    #INDEX FOR QUESTION 4
    elif(i == 4):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mid_pid ON Characters(mid,pid)")
    #INDEX FOR QUESTION 5
    elif(i == 5):
        cur.execute("CREATE INDEX IF NOT EXISTS idx_numVotes ON Ratings(numVotes)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pid ON Principals(pid)")
    #HANDLE POTENTIAL ERROR
    else:
        print("ERROR : question out of bound of questions list")

    
    connection.commit()


q1 = """SELECT primaryTitle 
FROM movies
JOIN principals ON movies.mid = principals.mid
JOIN persons ON principals.pid = persons.pid
WHERE primaryName = 'Jean Reno'
"""

q2 = """
SELECT primaryTitle
FROM movies
JOIN ratings ON movies.mid = ratings.mid
JOIN genres ON movies.mid = genres.mid
WHERE startYear BETWEEN 2000 AND 2010
AND genre = "Horror"
ORDER BY averageRating DESC LIMIT 3
"""

q3 = """
SELECT DISTINCT Persons.primaryName 
FROM Persons JOIN Writers ON Persons.pid = Writers.pid 
JOIN Titles ON Writers.mid = Titles.mid 
WHERE Titles.region != 'ES';
"""

q4 = """
SELECT Persons.primaryName, COUNT(*) AS nombreDeRoles 
FROM Persons JOIN Principals ON Persons.pid = Principals.pid 
GROUP BY Persons.pid 
ORDER BY nombreDeRoles DESC LIMIT 1;
"""

q5 = """
WITH connus_avant AS (
    SELECT DISTINCT pid
    FROM principals
    JOIN movies ON movies.mid = principals.mid
    JOIN ratings ON ratings.mid = movies.mid
    WHERE numvotes > 200000 AND startyear < 2009
),
connus_apres AS (
    SELECT DISTINCT pid
    FROM principals
    JOIN movies ON movies.mid = principals.mid
    JOIN ratings ON ratings.mid = movies.mid
    WHERE numvotes > 200000 AND startyear > 2009
),
connus_maintenant AS (
    SELECT DISTINCT pid 
    FROM connus_apres
    EXCEPT
    SELECT DISTINCT pid 
    FROM connus_avant
)
SELECT primaryName 
FROM persons
JOIN principals ON principals.pid = persons.pid
JOIN movies ON movies.mid = principals.mid
WHERE principals.pid IN connus_maintenant AND (primaryTitle = 'Avatar')
"""

questions = [q1,q2,q3,q4,q5]

for i in range (1,6):
    start_time = time.time()
    answer(i)
    print("DONE : Task took %s seconds" % (time.time() - start_time))

    #Add index for optimization
    add_index(i)

    start_time = time.time()
    answer(i)
    print("DONE : Task took %s seconds with index" % (time.time() - start_time))



