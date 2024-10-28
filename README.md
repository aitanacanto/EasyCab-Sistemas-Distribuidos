@descomentar: Código que hay que descomentar para que el programa haga su cometido (se utiliza para simplificar el programa y probar el funcionamiento de algunas partes de la misma).

@implementar: Código que será casi siempre un print que tendrá que ser implementada para el correcto funcionamiento del programa.

@KAFKA: Comunicaciones que había hecho con sockets y que han de ser cambiados por KAFKA

Para ejecutar KAFKA en el ordenador:

%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties

%KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties