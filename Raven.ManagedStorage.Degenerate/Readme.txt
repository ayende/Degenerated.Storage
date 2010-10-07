This is a simple implementation of a persistent managed storage for RavenDB.
The project is called degenerate because of the following factors:

* The implementation is thread safe, but it does so by aggresively locking when needed.
* All the keys are kepts in memory. If you have 10,000,000 items, and each item has a key of 32 bytes 
  this will consume ~300 MB just to hold the keys.
