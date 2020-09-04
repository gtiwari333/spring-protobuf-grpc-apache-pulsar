package gt.greeting;

import app.model.GreetingOuterClass.Greeting;
import app.model.PersonOuterClass.Person;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class GreetingApp {

    public static final String personTopic = "person-message";
    public static final String greetingTopic = "greeting-message";

    public static void main(String[] args) {
        SpringApplication.run(GreetingApp.class, args);
    }

    @Bean
    PulsarClient createPularClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

}

@Service
@Slf4j
@RequiredArgsConstructor
class GreetingService {

    final PulsarClient pulsarClient;

    @PostConstruct
    private void initConsumer() throws PulsarClientException {
        pulsarClient
                .newConsumer(Schema.PROTOBUF(Person.class))
                .topic(GreetingApp.personTopic)
                .subscriptionName("subscription-x1")
                .messageListener((consumer, p) -> handleMessage(p)).subscribe();
    }

    @SneakyThrows
    private void handleMessage(Message<Person> msg) {
        var p = msg.getValue();

        log.info("Received message: to convert {} ", p);

        var greeting = Greeting.newBuilder().setGreeting("Hello " + p.getFName() + " " + p.getLName()).build();

        //send back to the queue
        var producer = pulsarClient.newProducer(Schema.PROTOBUF(Greeting.class))
                .topic(GreetingApp.greetingTopic)
                .create();

        log.info("Sending back converted message {} ", greeting);
        producer.send(greeting);
    }


}
