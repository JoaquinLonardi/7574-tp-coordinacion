# Arquitectura y flujo del sistema

El gateway recibe los registros de cada cliente por socket y es el único lugar donde se les asigna una identidad. A cada conexión se le asigna un id único que se inyecta en el `message_handler`. A partir de ahí los registros viajan hacia la cola de Sum, donde se procesan en las distintas etapas. Una vez que se obtiene el resultado, este vuelve al gateway que a su vez se lo devuelve al cliente que lo pidió.

# Consultas Concurrentes

Para que múltiples clientes puedan consultar a la vez, cada mensaje lleva el id de la consulta a la que pertence y el resultado lo trae de vuelta. Cada etapa mantiene un dicccionario de estado por query id, por lo tanto los datos de un cliente nunca se cruzan con los datos de otro. Cuando el gateway recibe un resultado, le basta con comparar su query id con el de cada cliente y le entrega el resultado a quien corresonda. 

# Coordinación 

Las réplicas de Sum consumen la cola de entrada de forma competitiva, de a un mensaje por vez, de tal manera que los datos de una misma consulta quedan repartidos entre todas. El problema es que el fin de ingesta del cliente es un solo mensaje: lo recibe una única réplica, pero todas tienen parciales sin enviar.

Para coordinar el cierre se decidió utilizar una barrera. La réplica que recibe el fin de ingesta difunde, mediante un exchange de control de tipo fanout, un mensaje de flush a todas las réplicas y cada réplica, al recibir el flush, vuelca su parcial exactamente una vez: shardea sus frutas hacia las agregaciones y emite su propio fin de ingesta.

La correctitud se mantiene gracias a dos cosas. Primero, como el consumo es de a uno, el fin de ingesta es necesariamente el último mensaje de la cola de entrada, así que cuando una réplica lo recibe ya no quedan datos sin repartir. Segundo, cada réplica consume la cola de datos y la de control en un mismo hilo, de manera que el flush se procesa recién después del último dato que tenía en vuelo, y así ninguna fruta se pierde.

# Sharding hacia la agregación y fan-in en el join

En lugar de que cada Sum mande cada fruta a todas las agregaciones (lo que sería redundante), cada fruta se asigna siempre a la misma agregación. El reparto se hace con un hash entre procesos sobre el nombre de la fruta. De esta forma cada agregación recibe un subconjunto propio de frutas y no hay dos que procesen la misma.
Cada agregación consolida los parciales que le llegan de las distintas sumas y, mediante una barrera por conteo, espera el fin de ingesta de todas las réplicas de Sum antes de emitir. Lo que emite no es todo su estado sino solo su top parcial. Como toda fruta del top global está dentro del top de su partición, alcanza con mandar los primeros y se evita propagar datos de más.
El Join hace el fan-in. Acumula los tops parciales y, cuando recibió el de todas las agregaciones (otra barrera por conteo), los concatena y arma el top final. Como las particiones están disjuntas por fruta, no hay que volver a sumar nada, basta con juntar, ordenar con la Comparación de FruitItem y quedarse con los primeros.

# Escalabilidad

El sistema escala en tres ejes distintos:

- Frente a la cantidad de clientes concurrentes, el aislamiento por query id permite que muchas consultas convivan en el mismo pipeline sin interferir: cada etapa particiona su estado por consulta, así que sumar clientes no obliga a cambiar nada en la estructura.

- Para los grandes volúmenes de datos el trabajo se reparte. Las réplicas de Sum consumen la entrada de forma competitiva, cada mensaje se procesa a medida que llega en lugar de cargar todo en memoria, y el hecho de que cada etapa propague solo su top parcial y no todas sus frutas reduce el tráfico entre etapas.

- Ante la cantidad de controles, la partición disjunta por fruta hace que sumar réplicas de Aggregation reparta el trabajo sin que se pisen, aprovechando todas las instancias. Las réplicas de Sum se reparten la ingesta de la misma manera. El sistema crece agregando réplicas en el docker compose, sin tocar el código.
