=========================================
HTTP Sink connector Configuration Options
=========================================

Connection
^^^^^^^^^^

``http.url``
  The URL to send data to.

  * Type: string
  * Valid Values: HTTP(S) ULRs
  * Importance: high

``http.authorization.type``
  The HTTP authorization type.

  * Type: string
  * Valid Values: [none, static]
  * Importance: high
  * Dependents: ``http.headers.authorization``

``http.headers.authorization``
  The static content of Authorization header. Must be set along with 'static' authorization type.

  * Type: password
  * Default: null
  * Importance: medium

``http.headers.content.type``
  The value of Content-Type that will be send with each request.

  * Type: string
  * Default: null
  * Importance: low

Delivery
^^^^^^^^

``max.retries``
  The maximum number of times to retry on errors when sending a batch before failing the task.

  * Type: int
  * Default: 1
  * Valid Values: [0,...]
  * Importance: medium

``retry.backoff.ms``
  The time in milliseconds to wait following an error before a retry attempt is made.

  * Type: int
  * Default: 3000
  * Valid Values: [0,...]
  * Importance: medium

``max.outstanding.records``
  The maximum amount of records kept in memory by the connector waiting to be delivered. Serves for the back pressure.

  * Type: int
  * Default: 10000
  * Valid Values: [0,...]
  * Importance: low


