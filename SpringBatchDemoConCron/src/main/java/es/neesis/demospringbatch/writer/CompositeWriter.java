package es.neesis.demospringbatch.writer;

import es.neesis.demospringbatch.model.Operation;
import es.neesis.demospringbatch.model.UserEntity;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class CompositeWriter implements ItemWriter<Operation> {

    private final Map<String, ItemWriter<UserEntity>> writers;

    public CompositeWriter(DataSource dataSource) {
        writers = Map.of(
                "INSERT", new InsertWriter(dataSource),
                "UPDATE", new UpdateWriter(dataSource),
                "DELETE", new DeleteWriter(dataSource)
        );
    }

    @Override
    public void write(List<? extends Operation> list) throws Exception {
        for (Operation operation : list) {
            writers.get(operation.getOperation()).write(List.of(operation.getUserEntity()));
        }
    }
}
