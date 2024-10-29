@descomentar: Código que hay que descomentar para que el programa haga su cometido (se utiliza para simplificar el programa y probar el funcionamiento de algunas partes de la misma).

@implementar: Código que será casi siempre un print que tendrá que ser implementada para el correcto funcionamiento del programa.

@KAFKA: Comunicaciones que había hecho con sockets y que han de ser cambiados por KAFKA

Para ejecutar KAFKA en el ordenador en Windows:

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties

Para ejecutar KAFKA en el ordenador en Ubuntu:
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties