## cursus
The CursusDB cluster.


#### Default port is 7681

To run a cluster 
``` 
./cursus
```

if you don't have a ``.clusterconfig``, one will be created where you launch cursus.  The ``.clusterconfig`` is in yaml format.

You can add multiple nodes.  A node could be a pod, commodity serve, so forth.

``.clusterconfig:``
``` 
nodes:
    - 12.34.56.78:7682
    - 87.65.43.21:7682

```