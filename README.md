# ACDDL

A centralized download server

## RESTful API

### GET /nodes

* pattern (string)

    search nodes by the given pattern

### POST /nodes

sync ACD cache database

### DELETE /nodes/{id}

move the node to trash

### GET /cache

* nodes[] (string)

    compare nodes

### POST /cache

abort pending downloads

* (optional) acd_paths[] (string)

    pull files from the given paths

### PUT /cache/{id}

download the given node
