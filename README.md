rabbit-culler
=============

This is a prototyping project to determine the feasability of emitting events
from a PostgreSQL database, and having a daemon watch for these events to do
cache invalidation.

There are 2 parts to the project:

* `publisher.sql` - this SQL script connects to a RabbitMQ exchange and
  publishes event information. This informations consists of the table name, and
  the old and new row (or null where appropriate). This is all serialized to
  JSON.

* `uncache.hs` - the prototype consumer. This project would be part of the
  language specific data bindings to the database, and is a daemon that recieves
  events from RabbitMQ and invalidates the cache appropriately.

  For now, this is written to do uncaching from the MusicBrainz database.
