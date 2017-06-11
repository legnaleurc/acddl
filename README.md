# DDL

A centralized download server

## RESTful API

### GET /api/v1/nodes

* pattern (string)

    search nodes by the given pattern

### POST /api/v1/nodes

sync ACD cache database

### DELETE /api/v1/nodes/{id}

move the node to trash

### GET /api/v1/cache

* nodes[] (string)

    compare nodes

### POST /api/v1/cache

abort pending downloads

* (optional) acd_paths[] (string)

    pull files from the given paths

### PUT /api/v1/cache/{id}

download the given node
