package demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

public class BookTopologyBuilderTest {
    private  TopologyTestDriver testDriver;

	Serde<Book> bookSerde = SerdeFactory.getGenericSerde(new TypeReference<Book>() {
	});
    Serde<Set<Book>> bookSetSerde = SerdeFactory.getGenericSerde(new TypeReference<Set<Book>>() {
	});
    
    @BeforeEach
    public  void setUp() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "book-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100l);
        
        Topology topology = BookTopologyBuilder.getTopology();
        
        testDriver = new TopologyTestDriver(topology, props);
        
    }
    @AfterEach
    public void tearDown() {
    	testDriver.close();
    }

    @Test
    public void testSingleMessageWithNewKey() {
    	
    	Book lotr = new Book();
    	lotr.setName("Lord of the Rings");
    	lotr.setAuthorName("Tolkien");
 	
    	sendMessage(lotr.getName(), lotr);
    	sendMessage(lotr.getName(), lotr);
    	
    	List<Set<Book>> messages = new ArrayList<>();
    	
    	ProducerRecord<String, Set<Book>> tmp = null;
        
    	while((tmp = poll()) != null) {
    		messages.add(tmp.value());
    		System.out.println(tmp.value());
    	}
    	
    	Assertions.assertTrue(messages.size() <= 2);
        

    }
	private ProducerRecord<String, Set<Book>> poll() {
		ProducerRecord<String, Set<Book>> out = testDriver.readOutput("output", Serdes.String().deserializer(), bookSetSerde.deserializer());
		return out;
	}

	private void sendMessage(String key, Book book) {
		ConsumerRecordFactory<String, Book> factory = new ConsumerRecordFactory<>(Serdes.String().serializer(), bookSerde.serializer());
                
        testDriver.pipeInput(factory.create("input", key, book));
	}

}
