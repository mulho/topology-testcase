package demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.type.TypeReference;

public class SerdeFactory {
	public static <T> Serde<T> getGenericSerde(Class<T> typeReference) {
		return getGenericSerde(new TypeReference<T>() {});
	}
	

	public static <T> Serde<T> getGenericSerde(TypeReference<T> typeReference) {
		Map<String, Object> serdeProps = new HashMap<>();
		serdeProps.put("typeReference", typeReference);
		final Serializer<T> serializer = new JsonPOJOSerializer<>();
		serializer.configure(serdeProps, false);

		final Deserializer<T> deserializer = new JsonPOJODeserializer<>();
		deserializer.configure(serdeProps, false);

		final Serde<T> serde = Serdes.serdeFrom(serializer, deserializer);
		return serde;
	}
	
}
