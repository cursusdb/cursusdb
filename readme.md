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
- select with where clause (almost done)
- && queries (``select * from users where firstName == 'Alex' && age > 21;``)
- Finish delete
- Comments
- Encryption at rest
- Glitch with select n,n must fix
- Bring update to speed with select in regards to updates in logic.
- Each node should have a limit of documents. Say 10 million default per node.
- Cluster and nodes share same hashed key.
- Cluster to authenticate via basic authentication ``"username\0password`` with basic (read or read/write OR read/write/delete permissions)


### Cluster
Default port: 7681