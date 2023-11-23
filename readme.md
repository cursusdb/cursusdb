## Cursus Database System
JSON based, unstructured distributed database.  Capable of extremely fast parallel search.

## Query Language
### Selecting
```
select * from yourCollection where key == 'value';
select * from yourCollection where key != 'value';
select * from yourCollection where key != false;
select * from yourCollection where key >= 1;
select * from yourCollection where key <= 1;
select * from yourCollection where key < 1;
select * from yourCollection where key > 1.55;
select * from yourCollection where key > 1.55 && anotherKey != 'value' && anotherKey2 == "value";

```

### Inserting
``` 
insert into yourCollection({SIMPLE JSON})
insert into yourCollection({"email": "jdoe@example.com", "firstName": "John"}) 

```

When inserting you can check if the key and value already exists using ``!``
``` 
insert into yourCollection({"email!": "jdoe@example.com", "firstName": "John"}) 
``` 

email jdoe@example.com does not exist and there is no document with either 'audi' or 'honda' values within cars key array.
```
insert into yourCollection({"email!": "jdoe@example.com", "firstName": "John", "cars!": ["audi", "honda"]}) 
```

You can slice and limit

Limiting
``` 
select 4 from yourCollection where key == 'value';
```

Skipping 4 from first in collection
``` 
select 4,* from yourCollection where key == 'value';
```

Skipping 4 from first in collection and getting 2 documents
``` 
select 4,6 from yourCollection where key == 'value';
```

### Actions
- select
- update
- delete

## Todo
- Bring update to speed with select in regards to updates in logic.
- Finish delete
- Encryption at rest
- Comments
- Cluster and nodes share same hashed key.
- Cluster to authenticate via basic authentication ``"username\0password`` with basic (read or read/write OR read/write/delete permissions)


### Cluster
Default port: 7681


### Querying
``` 
Client: insert into users({"name!": "john", "age": 28});
Cluster: {"insert":{"$id":"4d96408f-16d6-452d-90d5-51fdcb6aabe5","age":28,"name":"john"},"message":"Document inserted","statusCode":2000}

Client: update 1 in users where name == 'john' && age > 16 with name = 'alex';
Cluster: 127.0.0.1:7224: {"lock":false,"message":"No documents updated.","statusCode":2000,"updated":null}
127.0.0.1:7223: {"lock":false,"message":"Document(s) updated.","statusCode":2000,"updated":[{"$id":"4d96408f-16d6-452d-90d5-51fdcb6aabe5","age":28,"name":"alex"}]}

Client: select * from users;
Client: select * from users where name == 'alex' && age >= 27;
```

using ``key!`` will make sure the value is unique across all nodes! 