package es.neesis.demospringbatch.writer;

import es.neesis.demospringbatch.model.UserEntity;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateWriter implements ItemWriter<UserEntity> {

    private final DataSource dataSource;
    public UpdateWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void write(List<? extends UserEntity> list) throws Exception {
        System.out.println("UpdateWriter.write");
        JdbcBatchItemWriter<UserEntity> builder = new JdbcBatchItemWriterBuilder<UserEntity>()
            .beanMapped()
            .sql("UPDATE users SET password = :password, email = :email WHERE username = :username")
            .dataSource(dataSource)
            .build();
        builder.afterPropertiesSet();

        List<UserEntity> userEntities = list.stream().map(UserEntity.class::cast).collect(Collectors.toList());
        builder.write(userEntities);
    }
}
