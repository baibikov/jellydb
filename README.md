# Jellydb in-memory NoSQL message database.
[![PkgGoDev](https://img.shields.io/badge/go.dev-docs-007d9c?style=flat-square&logo=go&logoColor=white)](https://pkg.go.dev/github.com/baibikov/jellydb)
[![GitHub release](https://img.shields.io/github/release/baibikov/jellyfish.svg?style=flat-square)](https://https://github.com/baibikov/jellydb/releases/latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/baibikov/jellydb?style=flat-square)](https://goreportcard.com/report/github.com/baibikov/jellydb)
[![Build Status](https://img.shields.io/github/workflow/status/baibikov/jellydb/Go?logo=github&style=flat-square)](https://github.com/baibikov/jellydb/actions?query=workflow)
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

#### Run tcp server on current port
```bash
go run cmd/tcp/main.go -addr :7777
```

#### Run CLI 
```bash
go run cmd/cli/main.go -addr :7777
```

#### Commands for use
```
(sys)
-help:  Navigating existing Commands
exit:  Exit from CLI
clear: Carriage cleaning

(store)
SET: Adding an entry to the read queue, as soon as the entry
example:
> SET my_super_important SOME_VALUE_1

GET [N]: Getting uncommitted messages from the batch queue and n is batch elements
example:
> GET my_super_important 2
> SOME_VALUE_1
> SOME_VALUE_2

COM [N]: Commenting on a batch of messages
example:
> COMMIT my_super_important 2
```

#### SET command:
```bash
> SET my_key_1 object_1
👌
> SET my_key_1 object_2
👌
> SET my_key_1 object_3
👌
```

#### GET command:
```bash
> GET my_key_1 3
object_1
object_2
object_3
```

#### COM (commit) command:
```bash
> COM my_key_1 3
👌
```
