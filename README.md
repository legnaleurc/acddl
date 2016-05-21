# ACDDL

A centralized download server

## RESTful API

### POST /download

* node_id (string)
    the node ID
* priority (integer)
    download priority, higher is higher
* need_mtime (integer)
    preserve modified time, 0 is false, other is true
