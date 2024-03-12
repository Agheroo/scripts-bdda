import sqlite3

connection = sqlite3.connect("database.db")
cur = connection.cursor()


def question(i):
    print("Question " + str(i) + " : ")
    res = cur.execute(q[i])
    for row in res:
        for elt in range (len(row)):
            print(row[elt])
            
            


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

q5 = """"""

q = [q1,q2,q3,q4,q5]

for i in range(len(question)):
    question(i)