package demo;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonPOJODeserializer<T> implements Deserializer<T> {
	private ObjectMapper objectMapper = new ObjectMapper();
	private TypeReference<T> typeReference;

	/**
	 * Default constructor needed by Kafka
	 */
	public JsonPOJODeserializer() {
	}

	public JsonPOJODeserializer(TypeReference<T> type) {
		this.typeReference = type;
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> props, boolean isKey) {
		typeReference = (TypeReference<T>) props.get("typeReference");
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		if (bytes == null || bytes.length == 0)
			return null;

		T data;
		try {
			data = objectMapper.readValue(bytes, typeReference);
		} catch (Exception e) {
			throw new SerializationException(e);
		}

		return data;
	}

	@Override
	public void close() {

	}
}