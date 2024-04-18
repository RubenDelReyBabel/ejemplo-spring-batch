package es.neesis.demospringbatch.tasklet;

import es.neesis.demospringbatch.config.batch.BatchConfiguration;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

@Component
public class DeleteFilesTasklet implements Tasklet {

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) {
        try {
            File dir = new ClassPathResource("users").getFile();
            Files
                    .walk(dir.toPath()) // Traverse the file tree in depth-first order
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            if (!Files.isDirectory(path)) {
                                Files.delete(path);
                                System.out.println("Deleted file: " + path);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return RepeatStatus.FINISHED;
    }
}
