# Jellydb in-memory NoSQL message database.

### Philosophy:
-----------
In-memory Database with the ability to upload / download data from file storage.

### File storage:
```bash
├── PATH_KEY
├────── STORAGE_MESSAGES_PATH_KEY
├──────────── log.jelly.db
└──────────── meta.jelly.format
```

> log.jelly.db:

A monotonically growing string of bytes having the following data set:
```bash
Message size: 4 bytes
Message: 512 bytes
```

Example:

When saving the message “my very important message” to the store, the message is converted to the following form:

```bash
10101my very important message\0\0\0\0\0\0\0\0….(up to 512)

size: 10101
message: my very important message\0\0\0\0\0\0\0\0….(up to 512)
```

>meta.jelly.format:

A file that contains the "meta" information of each key.
Stores the following data:
Offset of committed messages
Offset recorded messages

Example:

When saving the “my very important message” message to the repository and committing it, the message will be converted to the following form:

```bash
00010001
```

To decrypt a string, do the following:
```bash
take the first 4 bytes - the offset of the recorded messages
take the next 4 bytes - the offset of the comic messages
```

### Quick Start:

```bash
go get github.com/baibikov/jellydb
```
Code:

```go
package main

import (
	"fmt"
	"log"

	"github.com/baibikov/jellydb/jellystore"
)

func main() {
	store, err := jellystore.New(&jellystore.Config{
		Path: "YOU_STORE_PATH",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = store.Set("key", []byte("some_message"))
	if err != nil {
		log.Fatal(err)
	}

	bb, err := store.Get("key", 1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(bb)
}
```