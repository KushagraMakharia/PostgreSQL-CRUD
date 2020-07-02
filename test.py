from crud_opp import insert, update, delete, read


USER = "kushagra"
DB = "kushagra"
table = "marks"
#Insert Operation
insert(USER, DB, table, ("id", "name", "age"), (1, "Rahul", "16"))

#Read Operation
#read(USER, DB, table, 2)

#Update Operation
#update(USER, DB, table, 2, {"age":15, "name":"Raju"})

#Delete Operation
# delete(USER, DB, table, 1)
