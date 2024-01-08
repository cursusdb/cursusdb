## CursusDB E2E(End to end) Test
This custom E2E test goes through the entire systems functionality, simple as that!

The test will create a cluster with 2 nodes and 1 replica for each node.  The main nodes are configured to sync every minute.  All nodes are configured to log as well as cluster.  We test node recovery, the entire CDQL language and scenarios, we even relaying to an Observer.  Then clean up after the test is complete if passed.

This can be a pretty long running automated test but it hit's every function within the CursusDB system. 

![image.png](../images/e2e-image.png)