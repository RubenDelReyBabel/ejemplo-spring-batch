package es.neesis.demospringbatch.config.batch;

import es.neesis.demospringbatch.dto.OperationDto;
import es.neesis.demospringbatch.listener.UserExecutionListener;
import es.neesis.demospringbatch.model.Operation;
import es.neesis.demospringbatch.processor.OperationProcessor;
import es.neesis.demospringbatch.tasklet.DeleteFilesTasklet;
import es.neesis.demospringbatch.writer.CompositeWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    public final static String RESOURCES = "/users";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager txManager;

    public BatchConfiguration(JobRepository jobRepository, PlatformTransactionManager txManager) {
        this.jobRepository = jobRepository;
        this.txManager = txManager;
    }

    @Bean
    public ItemReader<OperationDto> reader() {
        FlatFileItemReader<OperationDto> delegate = new FlatFileItemReaderBuilder<OperationDto>()
                .name("userItemReader")
                .linesToSkip(1)
                .delimited()
                .names("operation", "username", "password", "email")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(OperationDto.class);
                }})
                .build();
        Resource[] resources = null;
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        try {
            resources = patternResolver.getResources("/users/*.csv");
        } catch (IOException e) {
            e.printStackTrace();
            resources = new Resource[0];
        }
        MultiResourceItemReader<OperationDto> reader = new MultiResourceItemReader<>();
        reader.setResources(resources);
        reader.setDelegate(delegate);
        return reader;
    }

    @Bean
    public OperationProcessor processor() {
        return new OperationProcessor();
    }

    @Bean
    public ItemWriter<Operation> writer(DataSource dataSource) {
        return new CompositeWriter(dataSource);
    }

    @Bean
    public Job importUserJob(UserExecutionListener listener, Step step1, Step step2) {
        String jobName = "importUserJob";
        return new JobBuilder(jobName)
                .repository(jobRepository)
                .listener(listener)
                .start(step1)
                .next(step2)
                .build();
    }

    @Bean
    public Step step1(ItemReader<OperationDto> reader, ItemWriter<Operation> writer, ItemProcessor<OperationDto, Operation> processor) {
        String stepName = "step1";
        return new StepBuilder(stepName)
                .repository(jobRepository)
                .transactionManager(txManager)
                .<OperationDto, Operation>chunk(2)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Step step2(DeleteFilesTasklet deleteFilesTasklet) {
        String stepName = "step2";
        return new StepBuilder(stepName)
                .repository(jobRepository)
                .transactionManager(txManager)
                .tasklet(deleteFilesTasklet)
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }


}
