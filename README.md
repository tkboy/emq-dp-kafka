
emq-kafka
=========

This is a simple emq kafka plugin. 2 topic can be configured:
* points - for client publish message.
* status - for client connection status.

Each topic can configure partition strategy and worker size seperately.

Plugin Config
-------------

Found Issue
-----------
When per partition workers size set too much or set to default(100),
an connection error may occur soon in a while with unknown reason, and block later reconnect.

Update:
after some investigation, it seems have something ralated to kafka settings, eg. replica count.

License
-------

Apache License Version 2.0
