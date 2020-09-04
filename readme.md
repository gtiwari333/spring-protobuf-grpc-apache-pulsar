# Sample Spring Boot app with GRPC Protobuf/ Apache Pulsar

It consists of two apps `main-app` and `greeting-service`. 
`main-app` takes person name via endpoint `/greet/{fName}/{lName}` which will be sent to the pulsar topic `person-message`. 
It will be received by another app `greeting-service` which will create a greeting messsage and puts to `greeting-message` topic which will be received by `main-app`.

`protobuf-model` is a common module that contains the Greeting.proto and Person.proto files and `protobuf-maven-plugin` to generate Java classes

Use the script on pulsar-docker.sh to start pulsar instance.
