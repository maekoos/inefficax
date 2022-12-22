# Inefficax
*Inefficient* is a toy database I wrote to learn more about B+-tree indexes and other database concepts. It currently handles index reads, writes and deletes pretty well, but has no real "object store". Instead it allocates one page per document, even if it is just a couple of bytes long. This results in enormous database files and a way too slow read, write and (especially) delete time.


## Benchmarks
Write, read and delete 10 000 objects (on an M1 Macbook Pro with 16GB ram):

```
Write time: 468.50925ms (46.85µs / insert)
Read time: 255.047625ms (25.504µs / read)
Delete time: 22.834804791s (2.28348ms / delete)
        Tree depth: 1
        Node count: 1
        Optimal size: 16kb (2 pages)
        Actual size: 82206kb (10035 pages)
```

Write, read and delete 100 000 keys in the index (on an M1 Macbook Pro with 16GB ram):
```
Write time: 4.520037083s (45.2µs / insert)
Read time: 3.905197791s (39.051µs / read)
Delete time: 4.56978425s (45.697µs / delete)
        Tree depth: 1
        Node count: 1
        Optimal size: 16kb (2 pages)
        Actual size: 2285kb (279 pages)
```


## TODO:
 - [ ] Update without first deleting
 - [ ] Object storage
   - [ ] Faster deletes
 - [ ] Shrink database file when the last page in file is freed


## Inspiration

 - https://rcoh.me/posts/postgres-indexes-under-the-hood/
 - https://github.com/nimrodshn/btree


## License
MIT License, see LICENSE file.