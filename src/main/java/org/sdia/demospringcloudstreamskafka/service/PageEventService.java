package org.sdia.demospringcloudstreamskafka.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.sdia.demospringcloudstreamskafka.entities.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {
    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input)->{
            System.out.println("_____________________");
            System.out.println(input.toString());
            System.out.println("_____________________");
        };
    }
    @Bean
    public Supplier<PageEvent>  pageEventSupplier(){
        return ()-> new PageEvent(
                Math.random()>0.5?"p1":"p2",
                Math.random()>0.5?"U1":"U2",
                new Date(),
                new Random().nextInt(90000));
    }
    @Bean
    public Function<PageEvent,PageEvent> pageEventFunction(){
        return (input)->{
            input.setName("Page Event");
            input.setUser("User1");
            return input;
        };
    }

    @Bean
    //kafka streams
    //KTable
    public Function<KStream</*key*/String,/*value*/PageEvent>,KStream<String,Long/*count*/>> KStreamFunction(){
        return (input)->{
            return input
                    .filter((k,v)->v.getDuration()>100)//to get just the pageevents which have the duration > 100
                    .map((k,v)->new KeyValue<>(v.getName(),0L))  //create new keyValue
                    .groupBy((k,v)->k/*group by key=groupByKey*/,Grouped.with(Serdes.String(),Serdes.Long()))
                    //Grouped.with(Serdes.String(), Serdes.Long()): This part specifies the Serdes (Serializer/Deserializer) for the key and value types of the grouped records. In this case, it indicates that the key type is String and the value type is Long.
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))//show the finally 5 pages //count ou les statistiques de 5 deriniers seconds == elle reteurn un key qui pas un string alors on fais map a la fine pour convertire le key a string
                    .count(Materialized.as("page-count"))//kafka vas produire une table page-count
                    .toStream()//pour convertire le ktable qui produire par group by a kstream
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }


}
