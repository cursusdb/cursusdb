## Cursus Database System
JSON based, unstructured distributed database.  Capable of extremely fast parallel search.

## Query Language
### Inserts
```
insert into users({"name": "Alex", "last": "Lee", "age": 28});
insert into users({"name": "John", "last": "Josh", "age": 28, "tags": ["tag1", "tag2"]});
```

### Selects
```
select * from users;
select 0,2 from users;
select 1 from users where name == 'Alex' || name == 'John';
select * from users where name == 'Alex' && age == 28;
select * from users where tags == "tag1";
select * from users where name == 'Alex' && age == 28 && tags == 'tag1';
```

### Updating
```
update 1 in users where age >= 28 set name = 'Josie';
update * in users where age > 24 && name == 'Alex' set name = 'Josie', age = 52;
update n, n..
ect..
```

### Deleting
```
delete * from users where age >= 28 || age < 32;
delete 0,5 from users where age > 28 && name == 'Alex';
ect
```

### Uniqueness
using ``key!`` will make sure the value is unique across all nodes!
``` 
insert into users({"email!": "test@example.com" ...});
```

### Operators
- ``>``
- ``>=``
- ``<``
- ``>=``
- ``==``
- ``!=``

### Conditionals
- ``&&``
- ``||``

### Actions
- ``select``
- ``update``
- ``delete``

### Status codes
#### Authentication 
- ``0`` Authentication successful.
- ``1`` Unable to read authentication header.
- ``2`` Invalid authentication value.
- ``3`` No user exists
#### Document
- ``2000`` Document inserted/updated/deleted
- ``4000`` Unmarsharable JSON insert
- ``4001`` Missing action
- ``4002`` None existent action
- ``4003`` Nested JSON object cannot be inserted
- ``4004`` Document already exists


## Todo
- Encryption at rest
- Comments
- Cluster and nodes share same hashed key.

### Ports
Default cluster port: 7681

Default node port: 7682

