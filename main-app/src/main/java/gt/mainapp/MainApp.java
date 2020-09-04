package gt.mainapp;

import app.model.GreetingOuterClass.Greeting;
import app.model.PersonOuterClass.Person;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class MainApp {

    public static final String personTopic = "person-message";
    public static final String greetingTopic = "greeting-message";

    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }

    @Bean
    PulsarClient pulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

    @Bean
    Producer<Person> personProducer(PulsarClient pulsarClient) throws PulsarClientException {
        return pulsarClient.newProducer(Schema.PROTOBUF(Person.class))
                .topic(personTopic)
                .create();
    }
}

@RestController
@Slf4j
@RequiredArgsConstructor
class AppController {

    final PulsarClient pulsarClient;
    final Producer<Person> personProducer;

    @PostConstruct
    private void initConsumer() throws PulsarClientException {
        pulsarClient
                .newConsumer(Schema.PROTOBUF(Greeting.class))
                .topic(MainApp.greetingTopic)
                .subscriptionName("subscription-2")
                .messageListener((consumer, msg) -> {

                    //message handler logic

                })
                .subscribe();

    }

    @GetMapping("/greet/{fName}/{lName}")
    @SneakyThrows
    String addMessage(@PathVariable String fName, @PathVariable String lName) {
        var person = Person.newBuilder()
                .setFName(fName)
                .setLName(lName)
                .build();

        log.info("Publishing to topic to convert");
        personProducer.send(person);

        return person + " is published to topic";
    }

}