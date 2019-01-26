# topology-testcase
KafkaStreams TopologyTestDriver bug test suite


To simulate the bug, just run `mvn package` which will run the test and fail.

Note that I send one message twice, and receive 3 messages as an output, and the second message (printed on the console) has an empty array, which is the result of the `subtractor` in the aggregate function.

# Background:
I have a topology that aggregates on a KTable. This is a generic method I created to build this topology on different topics I have.

```
public static <A, B, C> KTable<C, Set<B>> groupTable(KTable<A, B> table, Function<B, C> getKeyFunction,
        Serde<C> keySerde, Serde<B> valueSerde, Serde<Set<B>> aggregatedSerde) {
    return table
            .groupBy((key, value) -> KeyValue.pair(getKeyFunction.apply(value), value),
                    Serialized.with(keySerde, valueSerde))
            .aggregate(() -> new HashSet<>(), (key, newValue, agg) -> {
                agg.remove(newValue);
                agg.add(newValue);
                return agg;
            }, (key, oldValue, agg) -> {
                agg.remove(oldValue);
                return agg;
            }, Materialized.with(keySerde, aggregatedSerde));
}
```
This works pretty well when using Kafka, but not when testing via `TopologyTestDriver`.

In both scenarios, when I get an update, the subtractor is called first, and then the adder is called. The problem is that when using the TopologyTestDriver, two messages are sent out for updates: one after the subtractor call, and another one after the adder call. Not to mention that the message that is sent after the subrtractor and before the adder is in an incorrect stage.
