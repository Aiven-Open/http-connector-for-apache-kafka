=========================================
HTTP Sink connector Configuration Options
=========================================

Connection
^^^^^^^^^^

``http.url``
  The URL to send data to.

  * Type: string
  * Valid Values: HTTP(S) URL
  * Importance: high

``http.authorization.type``
  The HTTP authorization type.

  * Type: string
  * Valid Values: [none, oauth2, static]
  * Importance: high
  * Dependents: ``http.headers.authorization``

``http.headers.authorization``
  The static content of Authorization header. Must be set along with 'static' authorization type.

  * Type: password
  * Default: null
  * Importance: medium

``http.headers.content.type``
  The value of Content-Type that will be send with each request. Must be non-blank.

  * Type: string
  * Default: null
  * Valid Values: Non-blank string
  * Importance: low

``http.headers.additional``
  Additional headers to forward in the http request in the format header:value separated by a comma, headers are case-insensitive and no duplicate headers are allowed.

  * Type: list
  * Default: ""
  * Valid Values: Key value pair string list with format header:value
  * Importance: low

``oauth2.access.token.url``
  The URL to be used for fetching an access token. Client Credentials is the only supported grant type.

  * Type: string
  * Default: null
  * Valid Values: HTTP(S) URL
  * Importance: high
  * Dependents: ``oauth2.client.id``, ``oauth2.client.secret``, ``oauth2.client.authorization.mode``, ``oauth2.client.scope``, ``oauth2.response.token.property``

``oauth2.client.id``
  The client id used for fetching an access token.

  * Type: string
  * Default: null
  * Valid Values: OAuth2 client id
  * Importance: high
  * Dependents: ``oauth2.access.token.url``, ``oauth2.client.secret``, ``oauth2.client.authorization.mode``, ``oauth2.client.scope``, ``oauth2.response.token.property``

``oauth2.client.secret``
  The secret used for fetching an access token.

  * Type: password
  * Default: null
  * Importance: high
  * Dependents: ``oauth2.access.token.url``, ``oauth2.client.id``, ``oauth2.client.authorization.mode``, ``oauth2.client.scope``, ``oauth2.response.token.property``

``oauth2.client.authorization.mode``
  Specifies how to encode ``client_id`` and ``client_secret`` in the OAuth2 authorization request. If set to ``header``, the credentials are encoded as an ``Authorization: Basic <base-64 encoded client_id:client_secret>`` HTTP header. If set to ``url``, then ``client_id`` and ``client_secret`` are sent as URL encoded parameters. Default is ``header``.

  * Type: string
  * Default: HEADER
  * Valid Values: HEADER,URL
  * Importance: medium
  * Dependents: ``oauth2.access.token.url``, ``oauth2.client.id``, ``oauth2.client.secret``, ``oauth2.client.scope``, ``oauth2.response.token.property``

``oauth2.client.scope``
  The scope used for fetching an access token.

  * Type: string
  * Default: null
  * Valid Values: OAuth2 client scope
  * Importance: low
  * Dependents: ``oauth2.access.token.url``, ``oauth2.client.id``, ``oauth2.client.secret``, ``oauth2.client.authorization.mode``, ``oauth2.response.token.property``

``oauth2.response.token.property``
  The name of the JSON property containing the access token returned by the OAuth2 provider. Default value is ``access_token``.

  * Type: string
  * Default: access_token
  * Valid Values: OAuth2 response token
  * Importance: low
  * Dependents: ``oauth2.access.token.url``, ``oauth2.client.id``, ``oauth2.client.secret``, ``oauth2.client.authorization.mode``, ``oauth2.client.scope``

Batching
^^^^^^^^

``batching.enabled``
  Whether to enable batching multiple records in a single HTTP request.

  * Type: boolean
  * Default: false
  * Importance: high

``batch.max.size``
  The maximum size of a record batch to be sent in a single HTTP request.

  * Type: int
  * Default: 500
  * Valid Values: [1,...,1000000]
  * Importance: medium

``batch.prefix``
  Prefix added to record batches. Written once before the first record of a batch. Defaults to "" and may contain escape sequences like ``\n``.

  * Type: string
  * Default: ""
  * Importance: high

``batch.suffix``
  Suffix added to record batches. Written once after the last record of a batch. Defaults to "\n" (for backwards compatibility) and may contain escape sequences.

  * Type: string
  * Default: null
  * Importance: high

``batch.separator``
  Separator for records in a batch. Defaults to "\n" and may contain escape sequences.

  * Type: string
  * Default: null
  * Importance: high

Delivery
^^^^^^^^

``kafka.retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka Connect to retry delivering a message batch or performing recovery in case of transient failures.

  * Type: long
  * Default: null
  * Valid Values: null,[0, 86400000]
  * Importance: medium

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

Timeout
^^^^^^^

``http.timeout``
  HTTP Response timeout (seconds). Default is 30 seconds.

  * Type: int
  * Default: 30
  * Valid Values: [1,...]
  * Importance: low


