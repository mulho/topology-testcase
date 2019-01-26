/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * In this example, we implement a simple LineSplit program using the high-level
 * Streams DSL that reads from a source topic "streams-plaintext-input", where
 * the values of messages represent lines of text, and writes the messages as-is
 * into a sink topic "streams-pipe-output".
 */
public class BookTopologyBuilder {

	public static Topology getTopology() {
		final StreamsBuilder builder = new StreamsBuilder();
		Serde<Book> bookSerde = SerdeFactory.getGenericSerde(new TypeReference<Book>() {
		});
		Serde<Set<Book>> bookSetSerde = SerdeFactory.getGenericSerde(new TypeReference<Set<Book>>() {
		});
		KTable<String, Book> inputTable = builder.table("input", Consumed.with(Serdes.String(), bookSerde));
		
		KTable<String, Set<Book>> aggregatedTable = inputTable
				.groupBy((key, book) -> KeyValue.pair(book.getAuthorName(), book),
						Serialized.with(Serdes.String(), bookSerde))
				.aggregate(() -> new HashSet<>(), (key, newValue, agg) -> {
					agg.remove(newValue);
					agg.add(newValue);
					return agg;
				}, (key, oldValue, agg) -> {
					agg.remove(oldValue);
					return agg;
				}, Materialized.with(Serdes.String(), bookSetSerde));
		
		aggregatedTable.toStream().to("output", Produced.with(Serdes.String(), bookSetSerde));
		return builder.build();
	}
}

